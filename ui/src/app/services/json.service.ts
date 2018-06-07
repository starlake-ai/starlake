import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { TagDto } from '../dto/tag.dto';
import { ServerDto } from '../dto/server.dto';

@Injectable()
export class JsonService {

	private resourceTypes: Array<TagDto>;
	private servers: Array<ServerDto>;

	constructor(private http: HttpClient) { }

	getStaticResourceTypes() {
		return this.http.get<any>('assets/data/RT.json')
		.toPromise()
		.then(res => <TagDto[]>res)
		.then(data => { this.resourceTypes = data; return this.resourceTypes; });
	}

	getStaticServers() {
		return this.http.get<any>('assets/data/RTS.json')
		.toPromise()
		.then(res => <ServerDto[]>res)
		.then(data => { this.servers = data; return this.servers; });
	}

	addNewServer(params: any) {
		let server = {
			id: "" + (this.servers.length + 1),
			name: params.name,
			rts: []
		}
		this.servers.push(server);
		return this.servers;
	}

	addResourceType(id: string, params: any) {
		let server = this.servers.find(s => s.id == id);
		if(server){
			let rt = {
				id: "" + (server.rts.length + 1),
				tag: this.resourceTypes.find(r => r.id == params.resourceTypeId),
				podType: params.podType,
				cpu: params.cpu,
				memory: params.memory,
				count: params.count,
				dataLocation: params.dataLocation
			}
			this.servers.find(s => s.id == id).rts.push(rt);
		}
		return this.servers;
	}

	deleteResourceType(id: string, resourceTypeId: string) {
		let server = this.servers.find(s => s.id == id);
		if(server){
			let rts = server.rts.filter(rt => rt.id != resourceTypeId);
			this.servers.find(s => s.id == id).rts = rts;
		}
		return this.servers;
	}
}