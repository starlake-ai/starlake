import {FormControl} from '@angular/forms';
import * as $ from 'jquery';

import { ServerDto } from './dto/server.dto';
import { SortByPipe } from './tools/sort-by.pipe';

export abstract class CommonComponent {

	public showMask() {
		$(".main-mask").show();
	}

	public hideMask() {
		$(".main-mask").hide();
	}


	public validateCheckBox(c: FormControl) {
		return (c.value == true) ? null : {
			validateCheckBox: {
				valid: false
			}
		};
	}

	public getValidity(item: any) {
		let c = "";
		if (item && item.valid && item.dirty) {
			c = "valid";
		}
		else if (item && item.invalid && item.dirty) {
			c = "invalid"
		}
		return c;
	}

	public getSelectedServersByTagId(servers: Array<ServerDto>, tagId: string){
		let selectedServers = [];
		servers.map(s => {
			if(s.rts.filter(r => r.tag.id == tagId).length > 0){
				selectedServers.push(s);
			}
		});
		return selectedServers;
	}

	public getSortedServers(servers, selectedServers){
		let selectedServerIds = selectedServers.map(s => s.id);
		let unselectedServers = servers.filter(s => !selectedServerIds.includes(s.id));
		let sortedServers = new SortByPipe().transform(selectedServers, "name", false).concat(new SortByPipe().transform(unselectedServers, "name", false));
		return sortedServers;
	}
}