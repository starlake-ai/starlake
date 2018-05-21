import { TagDto } from './tag.dto';

export class ResourceTypeDto {

	public id: string;

	public tag: TagDto;

	public podType: string;

	public cpu: number;

	public memory: number;

	public count: number;

	public dataLocation: string;

	constructor(id?: string, tag?: TagDto, podType?: string, cpu?: number, memory?: number, count?: number, dataLocation?: string) {
		this.id = id;
		this.tag = tag;
		this.cpu = cpu;
		this.memory = memory;
		this.count = count;
		this.dataLocation = dataLocation;
	}
}