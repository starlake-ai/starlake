jsSnowball is a JavaScript port of http://snowball.tartarus.org/ stemmers,
web site is at: http://code.google.com/p/urim/

Summary:
========

snowball-js implements the popular "Snowball" word stemming algorithm in
JavaScript. Originally ported to JavaScript by Oleg Mazko for
[Urim](http://code.google.com/p/urim/), Fortnight Labs forked it to
GitHub to add support for Node and npm.


Install:
========

    npm install snowball

Usage:
======

    var Snowball = require('snowball');
    var stemmer = new Snowball('English');
    stemmer.setCurrent('abbreviations');
    stemmer.stem();
    console.log(stemmer.getCurrent());
    # => 'abbrebi'

Notes:
======
To make porting from Java sources easier each stemmer could be validated by Regex:

    [^\.]((limit_backward)|(limit)|(cursor)|(bra)|(ket)|(setCurrent)|(getCurrent)|(in_grouping)|(in_grouping_b)|(out_grouping)|(out_grouping_b)|(in_range)|(in_range_b)|(out_range)|(out_range_b)|(eq_s)|(eq_s_b)|(find_among)|(find_among_b)|(replace_s)|(slice_check)|(slice_from)|(slice_del)|(insert)|(slice_to)|(eq_v_b))

Snowball.min.js library was compressed by Google Closure Compiler:

    java -jar compiler.jar --js Snowball.js --js_output_file Snowball.min.js

Changes:
========

    0.3:
        in_range, in_range_b, out_range, out_range_b functions removed from sbp(never used).
    To decrease Snowball.js library size:
    Among constructor validation removed.
    setCurrent : function		->	s_c : function (1 match)
    this.setCurrent  		->	this.s_c (0 mathes)	(SnowballProgram only)
    sbp.setCurrent			->	sbp.s_c (15 mathes)
    getCurrent : function		->	g_c : function (1 match)
    this.getCurrent  		->	this.g_c (0 mathes)	(SnowballProgram only)
    sbp.getCurrent			->	sbp.g_c (15 mathes)
    in_grouping : function		->	i_g : function (1 match)
    this.in_grouping 		->	this.i_g (0 mathes)
    sbp.in_grouping			->	sbp.i_g (48 mathes)
    in_grouping_b : function	->	i_g_b : function (1 match)
    this.in_grouping_b 		->	this.i_g_b (0 mathes)
    sbp.in_grouping_b		->	sbp.i_g_b (24 mathes)
    out_grouping : function		->	o_g : function (1 match)
    this.out_grouping 		->	this.o_g (0 mathes)
    sbp.out_grouping		->	sbp.o_g (28 mathes)	
    out_grouping_b : function	->	o_g_b : function (1 match)
    this.out_grouping_b 		->	this.o_g_b (0 mathes)
    sbp.out_grouping_b		->	sbp.o_g_b (20 mathes)
    eq_s : function			->	e_s : function (1 match)
    this.eq_s 			->	this.e_s (0 mathes)
    sbp.eq_s			->	sbp.e_s (15 mathes)
    eq_s_b : function		->	e_s_b : function (1 match)
    this.eq_s_b 			->	this.e_s_b (1 mathes)
    sbp.eq_s_b			->	sbp.e_s_b (95 mathes)
    find_among : function		->	f_a : function (1 match)
    this.find_among 		->	this.f_a (0 mathes)
    sbp.find_among			->	sbp.f_a (14 mathes)
    find_among_b : function		->	f_a_b : function (1 match)
    this.find_among_b 		->	this.f_a_b (0 mathes)
    sbp.find_among_b		->	sbp.f_a_b (108 mathes)
    replace_s : function		->	r_s : function (1 match)
    this.replace_s	 		->	this.r_s (2 mathes)
    sbp.replace_s			->	sbp.r_s (0 mathes)
    slice_check : function		->	s_ch : function (1 match)
    this.slice_check	 	->	this.s_ch (2 mathes)
    sbp.slice_check			->	sbp.s_ch (0 mathes)
    slice_from : function		->	s_f : function (1 match)
    this.slice_from	 		->	this.s_f (1 mathes)
    sbp.slice_from			->	sbp.s_f (153 mathes)
    slice_del : function		->	s_d : function (1 match)
    this.slice_del	 		->	this.s_d (0 mathes)
    sbp.slice_del			->	sbp.s_d (204 mathes)
    insert : function		->	i_ : function (1 match)
    this.insert	 		->	this.i_ (0 mathes)
    sbp.insert			->	sbp.i_ (3 mathes)
    slice_to : function		->	s_t : function (1 match)
    this.slice_to	 		->	this.s_t (0 mathes)
    sbp.slice_to			->	sbp.s_t (2 mathes)
    eq_v_b : function		->	e_v_b : function (1 match)
    this.eq_v_b	 		->	this.e_v_b (0 mathes)
    sbp.eq_v_b			->	sbp.e_v_b (2 mathes)

    0.2:
    Refactoring - all code with loop labels replaced by more readable equivalent,
    unused variables (v_1, v_2 ... v_n) removed from all functions, 
    sometimes anonymous functions using as function parameter (Turkish).
    To decrease Snowball.js library size:
    among_var		->	a_v (308 mathes)
    bra : 0			->	b : 0 (1 match)
    this.bra 		->	this.b (7 mathes)
    sbp.bra			->	sbp.b (195 mathes)
    ket : 0			->	k : 0 (1 match)
    this.ket 		->	this.k (7 mathes)
    sbp.ket			->	sbp.k (178 mathes)
    limit : 0		->	l : 0 (1 match)
    this.limit 		->	this.l (11 mathes)
    sbp.limit		->	sbp.l (400 mathes)
    cursor : 0		->	c : 0 (1 match)
    this.cursor 		->	this.c (44 mathes)
    sbp.cursor		->	sbp.c (1093 mathes)
    limit_backward : 0	->	lb : 0 (1 match)
    this.limit_backward 	->	this.lb (7 mathes)
    sbp.limit_backward	->	sbp.lb (142 mathes)
    JsUnit 2.2 TestRunner(Pentium III 667 MHz, 384 Mb RAM) results:
    -------------------------------------------------------------------------------------------
    Running on Mozilla/5.0 (Windows; U; Windows NT 5.1; ru; rv:1.9.2.10) Gecko/20100914 Firefox/3.6.10 
    Status: Done (2257.108 seconds)
    Runs: 4846	Errors: 0	Failures: 0
    -------------------------------------------------------------------------------------------
    Running on Opera/9.80 (Windows NT 5.1; U; ru) Presto/2.6.30 Version/10.62
    Status: Done (7476.318 seconds)
    Runs: 4846	Errors: 0	Failures: 0
    -------------------------------------------------------------------------------------------
    Running on Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET4.0C; .NET4.0E; FDM)
    Status: Done (6027.738 seconds)
    Runs: 4846	 Errors: 0	Failures: 0
    -------------------------------------------------------------------------------------------

    0.1:
    First version. Porting from Java code with minimum changes. 
    Reflection for Finnish stemmer is replaced by using callback function as function parameter.
    JsUnit 2.2 TestRunner(Pentium III 667 MHz, 384 Mb RAM) results:
    Running on Mozilla/5.0 (Windows; U; Windows NT 5.1; ru; rv:1.9.2.10) Gecko/20100914 Firefox/3.6.10
    Status: Done (3458.767 seconds)
    Runs: 4846	Errors: 0	Failures: 0
    -------------------------------------------------------------------------------------------
    Running on Opera/9.80 (Windows NT 5.1; U; ru) Presto/2.6.30 Version/10.62
    Status: Done (7318.537 seconds)
    Runs: 4846	Errors: 0	Failures: 0
    -------------------------------------------------------------------------------------------
    Running on Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET4.0C; .NET4.0E; FDM)
    Status: Done (5562.278 seconds)
    Runs: 4846	Errors: 0	Failures: 0
