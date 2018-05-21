import { ResourceTypeDto } from './resourceType.dto';

export class ServerDto {

	public id: string;

	public name: string;

	public rts: Array<ResourceTypeDto>;

	constructor(id?: string, name?: string, rts?: Array<ResourceTypeDto>) {
		this.id = id;
		this.name = name;
		this.rts = rts;
	}
}