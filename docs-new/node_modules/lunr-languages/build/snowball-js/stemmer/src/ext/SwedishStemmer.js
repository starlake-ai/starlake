/*!
 * Snowball JavaScript Library v0.3
 * http://code.google.com/p/urim/
 * http://snowball.tartarus.org/
 *
 * Copyright 2010, Oleg Mazko
 * http://www.mozilla.org/MPL/
 */

function SwedishStemmer() {
	var a_0 = [new Among("a", -1, 1), new Among("arna", 0, 1),
			new Among("erna", 0, 1), new Among("heterna", 2, 1),
			new Among("orna", 0, 1), new Among("ad", -1, 1),
			new Among("e", -1, 1), new Among("ade", 6, 1),
			new Among("ande", 6, 1), new Among("arne", 6, 1),
			new Among("are", 6, 1), new Among("aste", 6, 1),
			new Among("en", -1, 1), new Among("anden", 12, 1),
			new Among("aren", 12, 1), new Among("heten", 12, 1),
			new Among("ern", -1, 1), new Among("ar", -1, 1),
			new Among("er", -1, 1), new Among("heter", 18, 1),
			new Among("or", -1, 1), new Among("s", -1, 2),
			new Among("as", 21, 1), new Among("arnas", 22, 1),
			new Among("ernas", 22, 1), new Among("ornas", 22, 1),
			new Among("es", 21, 1), new Among("ades", 26, 1),
			new Among("andes", 26, 1), new Among("ens", 21, 1),
			new Among("arens", 29, 1), new Among("hetens", 29, 1),
			new Among("erns", 21, 1), new Among("at", -1, 1),
			new Among("andet", -1, 1), new Among("het", -1, 1),
			new Among("ast", -1, 1)], a_1 = [new Among("dd", -1, -1),
			new Among("gd", -1, -1), new Among("nn", -1, -1),
			new Among("dt", -1, -1), new Among("gt", -1, -1),
			new Among("kt", -1, -1), new Among("tt", -1, -1)], a_2 = [
			new Among("ig", -1, 1), new Among("lig", 0, 1),
			new Among("els", -1, 1), new Among("fullt", -1, 3),
			new Among("l\u00F6st", -1, 2)], g_v = [17, 65, 16, 1, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0, 24, 0, 32], g_s_ending = [119, 127, 149], I_x, I_p1, sbp = new SnowballProgram();
	this.setCurrent = function(word) {
		sbp.setCurrent(word);
	};
	this.getCurrent = function() {
		return sbp.getCurrent();
	};
	function r_mark_regions() {
		var v_1, c = sbp.cursor + 3;
		I_p1 = sbp.limit;
		if (0 <= c || c <= sbp.limit) {
			I_x = c;
			while (true) {
				v_1 = sbp.cursor;
				if (sbp.in_grouping(g_v, 97, 246)) {
					sbp.cursor = v_1;
					break;
				}
				sbp.cursor = v_1;
				if (sbp.cursor >= sbp.limit)
					return;
				sbp.cursor++;
			}
			while (!sbp.out_grouping(g_v, 97, 246)) {
				if (sbp.cursor >= sbp.limit)
					return;
				sbp.cursor++;
			}
			I_p1 = sbp.cursor;
			if (I_p1 < I_x)
				I_p1 = I_x;
		}
	}
	function r_main_suffix() {
		var among_var, v_2 = sbp.limit_backward;
		if (sbp.cursor >= I_p1) {
			sbp.limit_backward = I_p1;
			sbp.cursor = sbp.limit;
			sbp.ket = sbp.cursor;
			among_var = sbp.find_among_b(a_0, 37);
			sbp.limit_backward = v_2;
			if (among_var) {
				sbp.bra = sbp.cursor;
				switch (among_var) {
					case 1 :
						sbp.slice_del();
						break;
					case 2 :
						if (sbp.in_grouping_b(g_s_ending, 98, 121))
							sbp.slice_del();
						break;
				}
			}
		}
	}
	function r_consonant_pair() {
		var v_1 = sbp.limit_backward;
		if (sbp.cursor >= I_p1) {
			sbp.limit_backward = I_p1;
			sbp.cursor = sbp.limit;
			if (sbp.find_among_b(a_1, 7)) {
				sbp.cursor = sbp.limit;
				sbp.ket = sbp.cursor;
				if (sbp.cursor > sbp.limit_backward) {
					sbp.bra = --sbp.cursor;
					sbp.slice_del();
				}
			}
			sbp.limit_backward = v_1;
		}
	}
	function r_other_suffix() {
		var among_var, v_2;
		if (sbp.cursor >= I_p1) {
			v_2 = sbp.limit_backward;
			sbp.limit_backward = I_p1;
			sbp.cursor = sbp.limit;
			sbp.ket = sbp.cursor;
			among_var = sbp.find_among_b(a_2, 5);
			if (among_var) {
				sbp.bra = sbp.cursor;
				switch (among_var) {
					case 1 :
						sbp.slice_del();
						break;
					case 2 :
						sbp.slice_from("l\u00F6s");
						break;
					case 3 :
						sbp.slice_from("full");
						break;
				}
			}
			sbp.limit_backward = v_2;
		}
	}
	this.stem = function() {
		var v_1 = sbp.cursor;
		r_mark_regions();
		sbp.limit_backward = v_1;
		sbp.cursor = sbp.limit;
		r_main_suffix();
		sbp.cursor = sbp.limit;
		r_consonant_pair();
		sbp.cursor = sbp.limit;
		r_other_suffix();
		return true;
	}
}