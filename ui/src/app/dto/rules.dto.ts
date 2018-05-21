export class RulesDto {

	public id: string;

	public parity: string;

	public min: number;

	public max: number;

	constructor(id?: string, parity?: string, min?: number, max?: number) {
		this.id = id;
		this.parity = parity;
		this.min = min;
		this.max = max;
	}
}
