import { RulesDto } from './rules.dto';

export class ResourceTypeDto {

	public id: string;

	public name: string;

	public rules: RulesDto;

	public podType: string;

	public cpu: number;

	public memory: number;

	public count: number;

	public dataLocation: string;

	constructor(id?: string, name?: string, rules?: RulesDto, podType?: string, cpu?: number, memory?: number, count?: number, dataLocation?: string) {
		this.id = id;
		this.name = name;
		this.rules = rules;
		this.cpu = cpu;
		this.memory = memory;
		this.count = count;
		this.dataLocation = dataLocation;
	}
}