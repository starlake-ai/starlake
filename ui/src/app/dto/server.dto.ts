import { ResourceTypeDto } from './resourceType.dto';

export class ServerDto {

	public id: string;

	public name: string;

	public RTS: Array<ResourceTypeDto>;

	constructor(id?: string, name?: string, RTS?: Array<ResourceTypeDto>) {
		this.id = id;
		this.name = name;
		this.RTS = RTS;
	}
}