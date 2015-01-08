/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "logging/logging.hpp"

#include "master/drf_sorter.hpp"

using std::list;
using std::set;
using std::string;

namespace mesos {
namespace internal {
namespace master {
namespace allocator {

bool DRFComparator::operator ()(const Client& client1, const Client& client2) {
	if (client1.share == client2.share) {
		if (client1.allocations == client2.allocations) {
			return client1.name < client2.name;
		}
		return client1.allocations < client2.allocations;
	}
	return client1.share > client2.share; // return the biggest share
}

void DRFSorter::add(const string& name, const string& role, double weight) {
	Client client(name, role, 0, 0);
	clients.insert(client);

	allocations[name] = Resources();
	weights[name] = weight;
	updateAllocations();
}

void DRFSorter::remove(const string& name) {
	set<Client, DRFComparator>::iterator it = find(name);

	if (it != clients.end()) {
		clients.erase(it);
	}

	allocations.erase(name);
	weights.erase(name);
	updateAllocations();
}

void DRFSorter::activate(const string& name, const string& role) {
	CHECK(allocations.contains(name));

	Client client(name, role, calculateShare(name), 0);
	clients.insert(client);
}

void DRFSorter::updateAllocations() {
	LOG(INFO)
			<< "updateAllocations method: set allocations to 0 for all frameworks";
	set<Client, DRFComparator>::iterator it;
	for (it = clients.begin(); it != clients.end(); it++) {
		Client client(*it);
		client.allocations = 0;
		clients.erase(it);
		clients.insert(client);
	}
}

void DRFSorter::deactivate(const string& name) {
	set<Client, DRFComparator>::iterator it = find(name);

	if (it != clients.end()) {
		// TODO(benh): Removing the client is an unfortuante strategy
		// because we lose information such as the number of allocations
		// for this client which means the fairness can be gamed by a
		// framework disconnecting and reconnecting.
		clients.erase(it);
		updateAllocations();
	}
}

void DRFSorter::allocated(const string& name, const Resources& resources) {
	set<Client, DRFComparator>::iterator it = find(name);

	if (it != clients.end()) { // TODO(benh): This should really be a CHECK.
			// TODO(benh): Refactor 'update' to be able to reuse it here.
		Client client(*it);

		// Update the 'allocations' to reflect the allocator decision.
		client.allocations++;
		LOG(INFO) << "allocate method: ROLE: " << client.role;

		// Remove and reinsert it to update the ordering appropriately.
		clients.erase(it);
		clients.insert(client);
	}

	allocations[name] += resources;

	// find the smallest allocation number of all frameworks with share 0

	unsigned int smallest = -1;
	set<Client, DRFComparator>::iterator it2;
	for (it2 = clients.begin(); it2 != clients.end(); it2++) {
		Client client(*it2);
		if (client.share != -1) {
			if (smallest == (unsigned int) -1) {
				smallest = client.allocations;
			} else {
				if (smallest > client.allocations) {
					smallest = client.allocations;
				}
			}
		}
		//clients.erase(it);
		//clients.insert(client);
	}

	std::set<Client, DRFComparator> clientsTemp;
	// set the smallest to all the framework with share -1,
	LOG(INFO) << "allocate method: smallest value: " << smallest;
	LOG(INFO)
			<< "allocate method: balance allocations for all the frameworks with share -1";
	set<Client, DRFComparator>::iterator it3;
	for (it3 = clients.begin(); it3 != clients.end(); it3++) {
		Client client(*it3);
		if (client.share == -1) {
			LOG(INFO) << "allocate method: set smallest value to client: "
					<< client.name;
			client.allocations = smallest;
		}
		clientsTemp.insert(client);
	}

	clients = clientsTemp;

// If the total resources have changed, we're going to
// recalculate all the shares, so don't bother just
// updating this client.
	if (!dirty) {
		update(name);
	}
}

Resources DRFSorter::allocation(const string& name) {
	return allocations[name];
}

void DRFSorter::unallocated(const string& name, const Resources& resources) {
	allocations[name] -= resources;

	if (!dirty) {
		update(name);
	}
}

void DRFSorter::add(const Resources& _resources) {
	resources += _resources;

// We have to recalculate all shares when the total resources
// change, but we put it off until sort is called
// so that if something else changes before the next allocation
// we don't recalculate everything twice.
	dirty = true;
}

void DRFSorter::remove(const Resources& _resources) {
	resources -= _resources;
	dirty = true;
}

list<string> DRFSorter::sort() {
	if (dirty) {
		set<Client, DRFComparator> temp;

		set<Client, DRFComparator>::iterator it;
		for (it = clients.begin(); it != clients.end(); it++) {
			Client client(*it);

			// Update the 'share' to get proper sorting.
			LOG(INFO) << "sort method: client name: " << client.name;
			LOG(INFO) << "sort method: client allocations: "
					<< client.allocations;
			if (client.role != "*") {
				client.share = 0;
			} else {
				client.share = calculateShare(client.name);
			}
			LOG(INFO) << "sort method: client share: " << client.share;
			LOG(INFO) << "sort method: client weight: " << weights[client.name];

			temp.insert(client);
		}
		// bug? what happens if a framework is added during sort
		clients = temp;
	}

	list<string> result;

	LOG(INFO) << "sort method: display the clients and allocations";

	set<Client, DRFComparator>::iterator it;
	for (it = clients.begin(); it != clients.end(); it++) {
		LOG(INFO) << "sort method: client name " << (*it).name;
		LOG(INFO) << "sort method: client allocations " << (*it).allocations;

		result.push_back((*it).name);
	}

	LOG(INFO) << "sort method: display the final order after push_back calls";

	for (string c : result) {
		LOG(INFO) << "sort method: result array: " << c;
	}

	return result;
}

bool DRFSorter::contains(const string& name) {
	return allocations.contains(name);
}

int DRFSorter::count() {
	return allocations.size();
}

void DRFSorter::update(const string& name) {
	set<Client, DRFComparator>::iterator it = find(name);

	if (it != clients.end()) {
		Client client(*it);

		// Update the 'share' to get proper sorting.
		LOG(INFO) << "update method: client name: " << client.name;
		if (client.role != "*") {
			client.share = 0;
		} else {
			client.share = calculateShare(client.name);
		}
		LOG(INFO) << "update method: client share: " << client.share;
		// Remove and reinsert it to update the ordering appropriately.
		clients.erase(it);
		clients.insert(client);
	}
}

double DRFSorter::calculateShare(const string& name) {
	double share = 0;

// TODO(benh): This implementaion of "dominant resource fairness"
// currently does not take into account resources that are not
// scalars.

// check if coarse mode.
// check if list size == 2
// remove mem share computation
	int cpu = 0;

	foreach (const Resource& resource, resources) {
		LOG(INFO) << "calculateShare method: resource name: " << resource.name();
		LOG(INFO) << "calculateShare method: resource type: " << resource.type();
		if (resource.type() == Value::SCALAR) {
			double tempShare = 0;
			double total = resource.scalar().value();
			LOG(INFO) << "calculateShare method: resource name: " << resource.name();
			LOG(INFO) << "calculateShare method: resource.scalar().value(): total: " << total;
			if (total > 0) {
				Value::Scalar none;
				const Value::Scalar& scalar =
				allocations[name].get(resource.name(), none);
				LOG(INFO) << "calculateShare method: scalar.value(): " << scalar.value();
				tempShare = scalar.value() / total;
				if(resource.name() == "mem") {
					LOG(INFO) << "calculateShare method: ignore mem resource for share: tempShare: " << tempShare;
				} else {
					share = std::max(share, tempShare);
				}
				LOG(INFO) << " calculateShare method: share: " << share;
			}
			if(resource.name() == "cpus" && share > 0) {
				cpu = -1;
			}
		}
	}
// if there is only one framework than return 0
	if (count() == 1) {
		LOG(INFO)
				<< "calculateShare method: if there is only one framework than return 0. Allocations size: "
				<< count();
		return 0;
	}

	LOG(INFO) << "calculateShare method: weights[name]: " << weights[name];

	if (cpu == -1) {
		LOG(INFO)
				<< "calculateShare method: coarse-grained detected or the framework has some cpus allocated to finish the current tasks";
		return -1;
	} else {
		return share / weights[name];
	}

}

set<Client, DRFComparator>::iterator DRFSorter::find(const string& name) {
	set<Client, DRFComparator>::iterator it;
	for (it = clients.begin(); it != clients.end(); it++) {
		if (name == (*it).name) {
			break;
		}
	}

	return it;
}

} // namespace allocator {
} // namespace master {
} // namespace internal {
} // namespace mesos {
