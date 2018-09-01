export class TagDto {

  public id: string;

  public parity: string;

  public min: string;

  public max: string;

  public name: string;

  constructor(id?: string, parity?: string, min?: string, max?: string, name?: string) {
    this.id = id;
    this.parity = parity;
    this.min = min;
    this.max = max;
    this.name = name;
  }
}
