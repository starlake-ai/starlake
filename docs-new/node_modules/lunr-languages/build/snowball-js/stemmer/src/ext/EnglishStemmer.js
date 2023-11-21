/*!
 * Snowball JavaScript Library v0.3
 * http://code.google.com/p/urim/
 * http://snowball.tartarus.org/
 *
 * Copyright 2010, Oleg Mazko
 * http://www.mozilla.org/MPL/
 */

function EnglishStemmer() {
	var a_0 = [new Among("arsen", -1, -1), new Among("commun", -1, -1),
			new Among("gener", -1, -1)], a_1 = [new Among("'", -1, 1),
			new Among("'s'", 0, 1), new Among("'s", -1, 1)], a_2 = [
			new Among("ied", -1, 2), new Among("s", -1, 3),
			new Among("ies", 1, 2), new Among("sses", 1, 1),
			new Among("ss", 1, -1), new Among("us", 1, -1)], a_3 = [
			new Among("", -1, 3), new Among("bb", 0, 2), new Among("dd", 0, 2),
			new Among("ff", 0, 2), new Among("gg", 0, 2),
			new Among("bl", 0, 1), new Among("mm", 0, 2),
			new Among("nn", 0, 2), new Among("pp", 0, 2),
			new Among("rr", 0, 2), new Among("at", 0, 1),
			new Among("tt", 0, 2), new Among("iz", 0, 1)], a_4 = [
			new Among("ed", -1, 2), new Among("eed", 0, 1),
			new Among("ing", -1, 2), new Among("edly", -1, 2),
			new Among("eedly", 3, 1), new Among("ingly", -1, 2)], a_5 = [
			new Among("anci", -1, 3), new Among("enci", -1, 2),
			new Among("ogi", -1, 13), new Among("li", -1, 16),
			new Among("bli", 3, 12), new Among("abli", 4, 4),
			new Among("alli", 3, 8), new Among("fulli", 3, 14),
			new Among("lessli", 3, 15), new Among("ousli", 3, 10),
			new Among("entli", 3, 5), new Among("aliti", -1, 8),
			new Among("biliti", -1, 12), new Among("iviti", -1, 11),
			new Among("tional", -1, 1), new Among("ational", 14, 7),
			new Among("alism", -1, 8), new Among("ation", -1, 7),
			new Among("ization", 17, 6), new Among("izer", -1, 6),
			new Among("ator", -1, 7), new Among("iveness", -1, 11),
			new Among("fulness", -1, 9), new Among("ousness", -1, 10)], a_6 = [
			new Among("icate", -1, 4), new Among("ative", -1, 6),
			new Among("alize", -1, 3), new Among("iciti", -1, 4),
			new Among("ical", -1, 4), new Among("tional", -1, 1),
			new Among("ational", 5, 2), new Among("ful", -1, 5),
			new Among("ness", -1, 5)], a_7 = [new Among("ic", -1, 1),
			new Among("ance", -1, 1), new Among("ence", -1, 1),
			new Among("able", -1, 1), new Among("ible", -1, 1),
			new Among("ate", -1, 1), new Among("ive", -1, 1),
			new Among("ize", -1, 1), new Among("iti", -1, 1),
			new Among("al", -1, 1), new Among("ism", -1, 1),
			new Among("ion", -1, 2), new Among("er", -1, 1),
			new Among("ous", -1, 1), new Among("ant", -1, 1),
			new Among("ent", -1, 1), new Among("ment", 15, 1),
			new Among("ement", 16, 1)], a_8 = [new Among("e", -1, 1),
			new Among("l", -1, 2)], a_9 = [new Among("succeed", -1, -1),
			new Among("proceed", -1, -1), new Among("exceed", -1, -1),
			new Among("canning", -1, -1), new Among("inning", -1, -1),
			new Among("earring", -1, -1), new Among("herring", -1, -1),
			new Among("outing", -1, -1)], a_10 = [new Among("andes", -1, -1),
			new Among("atlas", -1, -1), new Among("bias", -1, -1),
			new Among("cosmos", -1, -1), new Among("dying", -1, 3),
			new Among("early", -1, 9), new Among("gently", -1, 7),
			new Among("howe", -1, -1), new Among("idly", -1, 6),
			new Among("lying", -1, 4), new Among("news", -1, -1),
			new Among("only", -1, 10), new Among("singly", -1, 11),
			new Among("skies", -1, 2), new Among("skis", -1, 1),
			new Among("sky", -1, -1), new Among("tying", -1, 5),
			new Among("ugly", -1, 8)], g_v = [17, 65, 16, 1], g_v_WXY = [1, 17,
			65, 208, 1], g_valid_LI = [55, 141, 2], B_Y_found, I_p2, I_p1, habr = [
			r_Step_1b, r_Step_1c, r_Step_2, r_Step_3, r_Step_4, r_Step_5], sbp = new SnowballProgram();
	this.setCurrent = function(word) {
		sbp.setCurrent(word);
	};
	this.getCurrent = function() {
		return sbp.getCurrent();
	};
	function r_prelude() {
		var v_1 = sbp.cursor, v_2;
		B_Y_found = false;
		sbp.bra = sbp.cursor;
		if (sbp.eq_s(1, "'")) {
			sbp.ket = sbp.cursor;
			sbp.slice_del();
		}
		sbp.cursor = v_1;
		sbp.bra = v_1;
		if (sbp.eq_s(1, "y")) {
			sbp.ket = sbp.cursor;
			sbp.slice_from("Y");
			B_Y_found = true;
		}
		sbp.cursor = v_1;
		while (true) {
			v_2 = sbp.cursor;
			if (sbp.in_grouping(g_v, 97, 121)) {
				sbp.bra = sbp.cursor;
				if (sbp.eq_s(1, "y")) {
					sbp.ket = sbp.cursor;
					sbp.cursor = v_2;
					sbp.slice_from("Y");
					B_Y_found = true;
					continue;
				}
			}
			if (v_2 >= sbp.limit) {
				sbp.cursor = v_1;
				return;
			}
			sbp.cursor = v_2 + 1;
		}
	}
	function r_mark_regions() {
		var v_1 = sbp.cursor;
		I_p1 = sbp.limit;
		I_p2 = I_p1;
		if (!sbp.find_among(a_0, 3)) {
			sbp.cursor = v_1;
			if (habr1()) {
				sbp.cursor = v_1;
				return;
			}
		}
		I_p1 = sbp.cursor;
		if (!habr1())
			I_p2 = sbp.cursor;
	}
	function habr1() {
		while (!sbp.in_grouping(g_v, 97, 121)) {
			if (sbp.cursor >= sbp.limit)
				return true;
			sbp.cursor++;
		}
		while (!sbp.out_grouping(g_v, 97, 121)) {
			if (sbp.cursor >= sbp.limit)
				return true;
			sbp.cursor++;
		}
		return false;
	}
	function r_shortv() {
		var v_1 = sbp.limit - sbp.cursor;
		if (!(sbp.out_grouping_b(g_v_WXY, 89, 121)
				&& sbp.in_grouping_b(g_v, 97, 121) && sbp.out_grouping_b(g_v,
				97, 121))) {
			sbp.cursor = sbp.limit - v_1;
			if (!sbp.out_grouping_b(g_v, 97, 121)
					|| !sbp.in_grouping_b(g_v, 97, 121)
					|| sbp.cursor > sbp.limit_backward)
				return false;
		}
		return true;
	}
	function r_R1() {
		return I_p1 <= sbp.cursor;
	}
	function r_R2() {
		return I_p2 <= sbp.cursor;
	}
	function r_Step_1a() {
		var among_var, v_1 = sbp.limit - sbp.cursor;
		sbp.ket = sbp.cursor;
		among_var = sbp.find_among_b(a_1, 3);
		if (among_var) {
			sbp.bra = sbp.cursor;
			if (among_var == 1)
				sbp.slice_del();
		} else
			sbp.cursor = sbp.limit - v_1;
		sbp.ket = sbp.cursor;
		among_var = sbp.find_among_b(a_2, 6);
		if (among_var) {
			sbp.bra = sbp.cursor;
			switch (among_var) {
				case 1 :
					sbp.slice_from("ss");
					break;
				case 2 :
					var c = sbp.cursor - 2;
					if (sbp.limit_backward > c || c > sbp.limit) {
						sbp.slice_from("ie");
						break;
					}
					sbp.cursor = c;
					sbp.slice_from("i");
					break;
				case 3 :
					do {
						if (sbp.cursor <= sbp.limit_backward)
							return;
						sbp.cursor--;
					} while (!sbp.in_grouping_b(g_v, 97, 121));
					sbp.slice_del();
					break;
			}
		}
	}
	function r_Step_1b() {
		var among_var, v_1, v_3, v_4;
		sbp.ket = sbp.cursor;
		among_var = sbp.find_among_b(a_4, 6);
		if (among_var) {
			sbp.bra = sbp.cursor;
			switch (among_var) {
				case 1 :
					if (r_R1())
						sbp.slice_from("ee");
					break;
				case 2 :
					v_1 = sbp.limit - sbp.cursor;
					while (!sbp.in_grouping_b(g_v, 97, 121)) {
						if (sbp.cursor <= sbp.limit_backward)
							return;
						sbp.cursor--;
					}
					sbp.cursor = sbp.limit - v_1;
					sbp.slice_del();
					v_3 = sbp.limit - sbp.cursor;
					among_var = sbp.find_among_b(a_3, 13);
					if (among_var) {
						sbp.cursor = sbp.limit - v_3;
						switch (among_var) {
							case 1 :
								var c = sbp.cursor;
								sbp.insert(sbp.cursor, sbp.cursor, "e");
								sbp.cursor = c;
								break;
							case 2 :
								sbp.ket = sbp.cursor;
								if (sbp.cursor > sbp.limit_backward) {
									sbp.cursor--;
									sbp.bra = sbp.cursor;
									sbp.slice_del();
								}
								break;
							case 3 :
								if (sbp.cursor == I_p1) {
									v_4 = sbp.limit - sbp.cursor;
									if (r_shortv()) {
										sbp.cursor = sbp.limit - v_4;
										var c = sbp.cursor;
										sbp.insert(sbp.cursor, sbp.cursor, "e");
										sbp.cursor = c;
									}
								}
								break;
						}
					}
					break;
			}
		}
	}
	function r_Step_1c() {
		var v_1 = sbp.limit - sbp.cursor;
		sbp.ket = sbp.cursor;
		if (!sbp.eq_s_b(1, "y")) {
			sbp.cursor = sbp.limit - v_1;
			if (!sbp.eq_s_b(1, "Y"))
				return;
		}
		sbp.bra = sbp.cursor;
		if (sbp.out_grouping_b(g_v, 97, 121) && sbp.cursor > sbp.limit_backward)
			sbp.slice_from("i");
	}
	function r_Step_2() {
		var among_var;
		sbp.ket = sbp.cursor;
		among_var = sbp.find_among_b(a_5, 24);
		if (among_var) {
			sbp.bra = sbp.cursor;
			if (r_R1()) {
				switch (among_var) {
					case 1 :
						sbp.slice_from("tion");
						break;
					case 2 :
						sbp.slice_from("ence");
						break;
					case 3 :
						sbp.slice_from("ance");
						break;
					case 4 :
						sbp.slice_from("able");
						break;
					case 5 :
						sbp.slice_from("ent");
						break;
					case 6 :
						sbp.slice_from("ize");
						break;
					case 7 :
						sbp.slice_from("ate");
						break;
					case 8 :
						sbp.slice_from("al");
						break;
					case 9 :
						sbp.slice_from("ful");
						break;
					case 10 :
						sbp.slice_from("ous");
						break;
					case 11 :
						sbp.slice_from("ive");
						break;
					case 12 :
						sbp.slice_from("ble");
						break;
					case 13 :
						if (sbp.eq_s_b(1, "l"))
							sbp.slice_from("og");
						break;
					case 14 :
						sbp.slice_from("ful");
						break;
					case 15 :
						sbp.slice_from("less");
						break;
					case 16 :
						if (sbp.in_grouping_b(g_valid_LI, 99, 116))
							sbp.slice_del();
						break;
				}
			}
		}
	}
	function r_Step_3() {
		var among_var;
		sbp.ket = sbp.cursor;
		among_var = sbp.find_among_b(a_6, 9);
		if (among_var) {
			sbp.bra = sbp.cursor;
			if (r_R1()) {
				switch (among_var) {
					case 1 :
						sbp.slice_from("tion");
						break;
					case 2 :
						sbp.slice_from("ate");
						break;
					case 3 :
						sbp.slice_from("al");
						break;
					case 4 :
						sbp.slice_from("ic");
						break;
					case 5 :
						sbp.slice_del();
						break;
					case 6 :
						if (r_R2())
							sbp.slice_del();
						break;
				}
			}
		}
	}
	function r_Step_4() {
		var among_var, v_1;
		sbp.ket = sbp.cursor;
		among_var = sbp.find_among_b(a_7, 18);
		if (among_var) {
			sbp.bra = sbp.cursor;
			if (r_R2()) {
				switch (among_var) {
					case 1 :
						sbp.slice_del();
						break;
					case 2 :
						v_1 = sbp.limit - sbp.cursor;
						if (!sbp.eq_s_b(1, "s")) {
							sbp.cursor = sbp.limit - v_1;
							if (!sbp.eq_s_b(1, "t"))
								return;
						}
						sbp.slice_del();
						break;
				}
			}
		}
	}
	function r_Step_5() {
		var among_var, v_1;
		sbp.ket = sbp.cursor;
		among_var = sbp.find_among_b(a_8, 2);
		if (among_var) {
			sbp.bra = sbp.cursor;
			switch (among_var) {
				case 1 :
					v_1 = sbp.limit - sbp.cursor;
					if (!r_R2()) {
						sbp.cursor = sbp.limit - v_1;
						if (!r_R1() || r_shortv())
							return;
						sbp.cursor = sbp.limit - v_1;
					}
					sbp.slice_del();
					break;
				case 2 :
					if (!r_R2() || !sbp.eq_s_b(1, "l"))
						return;
					sbp.slice_del();
					break;
			}
		}
	}
	function r_exception2() {
		sbp.ket = sbp.cursor;
		if (sbp.find_among_b(a_9, 8)) {
			sbp.bra = sbp.cursor;
			return sbp.cursor <= sbp.limit_backward;
		}
		return false;
	}
	function r_exception1() {
		var among_var;
		sbp.bra = sbp.cursor;
		among_var = sbp.find_among(a_10, 18);
		if (among_var) {
			sbp.ket = sbp.cursor;
			if (sbp.cursor >= sbp.limit) {
				switch (among_var) {
					case 1 :
						sbp.slice_from("ski");
						break;
					case 2 :
						sbp.slice_from("sky");
						break;
					case 3 :
						sbp.slice_from("die");
						break;
					case 4 :
						sbp.slice_from("lie");
						break;
					case 5 :
						sbp.slice_from("tie");
						break;
					case 6 :
						sbp.slice_from("idl");
						break;
					case 7 :
						sbp.slice_from("gentl");
						break;
					case 8 :
						sbp.slice_from("ugli");
						break;
					case 9 :
						sbp.slice_from("earli");
						break;
					case 10 :
						sbp.slice_from("onli");
						break;
					case 11 :
						sbp.slice_from("singl");
						break;
				}
				return true;
			}
		}
		return false;
	}
	function r_postlude() {
		var v_1;
		if (B_Y_found) {
			while (true) {
				v_1 = sbp.cursor;
				sbp.bra = v_1;
				if (sbp.eq_s(1, "Y")) {
					sbp.ket = sbp.cursor;
					sbp.cursor = v_1;
					sbp.slice_from("y");
					continue;
				}
				sbp.cursor = v_1;
				if (sbp.cursor >= sbp.limit)
					return;
				sbp.cursor++;
			}
		}
	}
	this.stem = function() {
		var v_1 = sbp.cursor;
		if (!r_exception1()) {
			sbp.cursor = v_1;
			var c = sbp.cursor + 3;
			if (0 <= c && c <= sbp.limit) {
				sbp.cursor = v_1;
				r_prelude();
				sbp.cursor = v_1;
				r_mark_regions();
				sbp.limit_backward = v_1;
				sbp.cursor = sbp.limit;
				r_Step_1a();
				sbp.cursor = sbp.limit;
				if (!r_exception2())
					for (var i = 0; i < habr.length; i++) {
						sbp.cursor = sbp.limit;
						habr[i]();
					}
				sbp.cursor = sbp.limit_backward;
				r_postlude();
			}
		}
		return true;
	}
}