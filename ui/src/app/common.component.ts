import {FormControl} from '@angular/forms';
import * as $ from 'jquery';

export abstract class CommonComponent {

	showMask() {
		$(".main-mask").show();
	}

	hideMask() {
		$(".main-mask").hide();
	}


	validateCheckBox(c: FormControl) {
		return (c.value == true) ? null : {
			validateCheckBox: {
				valid: false
			}
		};
	}

	getValidity(item: any) {
		let c = "";
		if (item && item.valid && item.dirty) {
			c = "valid";
		}
		else if (item && item.invalid && item.dirty) {
			c = "invalid"
		}
		return c;
	}

}