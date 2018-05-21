export class TagDto {

	public id: string;

	public parity: string;

	public min: number;

	public max: number;

	public name: string;

	constructor(id?: string, parity?: string, min?: number, max?: number, name?: string) {
		this.id = id;
		this.parity = parity;
		this.min = min;
		this.max = max;
		this.name = name;
	}
}
