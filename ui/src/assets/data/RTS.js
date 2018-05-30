var staticRTS = [{
	"id": "1",
	"name": "Server A",
	"rts":[{
		"id": "1",
		"tag":{
			"id": "2",
			"parity": "even",
			"min": "1",
			"max": "3",
			"name": "SQL Storage SLAVE"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "3",
		"dataLocation": "./test/test2"
	},{
		"id": "2",
		"tag":{
			"id": "3",
			"parity": "even",
			"min": "2",
			"max": "9",
			"name": "KAFKA incl. Zoo Keeper"
		},
		"podType": "StateLess",
		"cpu": "2",
		"memory": "1",
		"count": "1",
		"dataLocation": ""
	},{
		"id": "3",
		"name": "SQL MASTER",
		"tag":{
			"id": "1",
			"parity": "odd",
			"min": "1",
			"max": "3",
			"name": "SQL Storage MASTER"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "1",
		"dataLocation": "./test/test2"
	}]
},{
	"id": "2",
	"name": "Server B",
	"rts":[{
		"id": "1",
		"tag":{
			"id": "4",
			"parity": "odd",
			"min": "1",
			"max": "1",
			"name": "Elastic Search"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "1",
		"dataLocation": "./test/test2"
	},{
		"id": "2",
		"tag":{
			"id": "7",
			"parity": "odd",
			"min": "1",
			"max": "3",
			"name": "Micro Service"
		},
		"podType": "StateLess",
		"cpu": "2",
		"memory": "1",
		"count": "1",
		"dataLocation": ""
	}]
},{
	"id": "3",
	"name": "Server C",
	"rts":[{
		"id": "1",
		"tag":{
			"id": "4",
			"parity": "odd",
			"min": "1",
			"max": "1",
			"name": "Elastic Search"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "1",
		"dataLocation": "./test/test2"
	},{
		"id": "2",
		"tag":{
			"id": "5",
			"parity": "odd",
			"min": "1",
			"max": "3",
			"name": "HDFS Master Node"
		},
		"podType": "StateLess",
		"cpu": "2",
		"memory": "1",
		"count": "3",
		"dataLocation": ""
	},{
		"id": "3",
		"tag":{
			"id": "8",
			"parity": "even",
			"min": "1",
			"max": "3",
			"name": "JENKINS"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "3",
		"dataLocation": "./test/test2"
	},{
		"id": "4",
		"tag":{
			"id": "6",
			"parity": "none",
			"min": "2",
			"max": "3",
			"name": "HDFS Data Node"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "2",
		"dataLocation": "./test/test2"
	}]
},{
	"id": "4",
	"name": "Server D",
	"rts":[{
		"id": "1",
		"tag":{
			"id": "6",
			"parity": "none",
			"min": "2",
			"max": "3",
			"name": "HDFS Data Node"
		},
		"podType": "StateFull",
		"cpu": "6",
		"memory": "0.5",
		"count": "2",
		"dataLocation": "./test/test2"
	}]
},{
	"id": "5",
	"name": "Server E",
	"rts":[{
		"id": "1",
		"tag":{
			"id": "2",
			"parity": "even",
			"min": "1",
			"max": "3",
			"name": "SQL Storage SLAVE"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "3",
		"dataLocation": "./test/test2"
	},{
		"id": "2",
		"tag":{
			"id": "3",
			"parity": "even",
			"min": "2",
			"max": "9",
			"name": "KAFKA incl. Zoo Keeper"
		},
		"podType": "StateLess",
		"cpu": "2",
		"memory": "1",
		"count": "1",
		"dataLocation": ""
	},{
		"id": "3",
		"name": "SQL MASTER",
		"tag":{
			"id": "1",
			"parity": "odd",
			"min": "1",
			"max": "3",
			"name": "SQL Storage MASTER"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "1",
		"dataLocation": "./test/test2"
	}]
},{
	"id": "6",
	"name": "Server F",
	"rts":[{
		"id": "1",
		"tag":{
			"id": "4",
			"parity": "odd",
			"min": "1",
			"max": "1",
			"name": "Elastic Search"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "1",
		"dataLocation": "./test/test2"
	},{
		"id": "2",
		"tag":{
			"id": "7",
			"parity": "odd",
			"min": "1",
			"max": "3",
			"name": "Micro Service"
		},
		"podType": "StateLess",
		"cpu": "2",
		"memory": "1",
		"count": "1",
		"dataLocation": ""
	}]
},{
	"id": "7",
	"name": "Server G",
	"rts":[{
		"id": "1",
		"tag":{
			"id": "4",
			"parity": "odd",
			"min": "1",
			"max": "1",
			"name": "Elastic Search"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "1",
		"dataLocation": "./test/test2"
	},{
		"id": "2",
		"tag":{
			"id": "5",
			"parity": "odd",
			"min": "1",
			"max": "3",
			"name": "HDFS Master Node"
		},
		"podType": "StateLess",
		"cpu": "2",
		"memory": "1",
		"count": "3",
		"dataLocation": ""
	},{
		"id": "3",
		"tag":{
			"id": "8",
			"parity": "even",
			"min": "1",
			"max": "3",
			"name": "JENKINS"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "3",
		"dataLocation": "./test/test2"
	},{
		"id": "4",
		"tag":{
			"id": "6",
			"parity": "none",
			"min": "2",
			"max": "3",
			"name": "HDFS Data Node"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "2",
		"dataLocation": "./test/test2"
	}]
},{
	"id": "8",
	"name": "Server H",
	"rts":[{
		"id": "1",
		"tag":{
			"id": "6",
			"parity": "none",
			"min": "2",
			"max": "3",
			"name": "HDFS Data Node"
		},
		"podType": "StateFull",
		"cpu": "6",
		"memory": "0.5",
		"count": "2",
		"dataLocation": "./test/test2"
	}]
},{
	"id": "9",
	"name": "Server E",
	"rts":[{
		"id": "1",
		"tag":{
			"id": "2",
			"parity": "even",
			"min": "1",
			"max": "3",
			"name": "SQL Storage SLAVE"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "3",
		"dataLocation": "./test/test2"
	},{
		"id": "2",
		"tag":{
			"id": "3",
			"parity": "even",
			"min": "2",
			"max": "9",
			"name": "KAFKA incl. Zoo Keeper"
		},
		"podType": "StateLess",
		"cpu": "2",
		"memory": "1",
		"count": "1",
		"dataLocation": ""
	},{
		"id": "3",
		"name": "SQL MASTER",
		"tag":{
			"id": "1",
			"parity": "odd",
			"min": "1",
			"max": "3",
			"name": "SQL Storage MASTER"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "1",
		"dataLocation": "./test/test2"
	}]
},{
	"id": "10",
	"name": "Server J",
	"rts":[{
		"id": "1",
		"tag":{
			"id": "4",
			"parity": "odd",
			"min": "1",
			"max": "1",
			"name": "Elastic Search"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "1",
		"dataLocation": "./test/test2"
	},{
		"id": "2",
		"tag":{
			"id": "7",
			"parity": "odd",
			"min": "1",
			"max": "3",
			"name": "Micro Service"
		},
		"podType": "StateLess",
		"cpu": "2",
		"memory": "1",
		"count": "1",
		"dataLocation": ""
	}]
},{
	"id": "11",
	"name": "Server K",
	"rts":[{
		"id": "1",
		"tag":{
			"id": "4",
			"parity": "odd",
			"min": "1",
			"max": "1",
			"name": "Elastic Search"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "1",
		"dataLocation": "./test/test2"
	},{
		"id": "2",
		"tag":{
			"id": "5",
			"parity": "odd",
			"min": "1",
			"max": "3",
			"name": "HDFS Master Node"
		},
		"podType": "StateLess",
		"cpu": "2",
		"memory": "1",
		"count": "3",
		"dataLocation": ""
	},{
		"id": "3",
		"tag":{
			"id": "8",
			"parity": "even",
			"min": "1",
			"max": "3",
			"name": "JENKINS"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "3",
		"dataLocation": "./test/test2"
	},{
		"id": "4",
		"tag":{
			"id": "6",
			"parity": "none",
			"min": "2",
			"max": "3",
			"name": "HDFS Data Node"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "2",
		"dataLocation": "./test/test2"
	}]
},{
	"id": "12",
	"name": "Server L",
	"rts":[{
		"id": "1",
		"tag":{
			"id": "6",
			"parity": "none",
			"min": "2",
			"max": "3",
			"name": "HDFS Data Node"
		},
		"podType": "StateFull",
		"cpu": "6",
		"memory": "0.5",
		"count": "2",
		"dataLocation": "./test/test2"
	}]
},{
	"id": "13",
	"name": "Server M",
	"rts":[{
		"id": "1",
		"tag":{
			"id": "2",
			"parity": "even",
			"min": "1",
			"max": "3",
			"name": "SQL Storage SLAVE"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "3",
		"dataLocation": "./test/test2"
	},{
		"id": "2",
		"tag":{
			"id": "3",
			"parity": "even",
			"min": "2",
			"max": "9",
			"name": "KAFKA incl. Zoo Keeper"
		},
		"podType": "StateLess",
		"cpu": "2",
		"memory": "1",
		"count": "1",
		"dataLocation": ""
	},{
		"id": "3",
		"name": "SQL MASTER",
		"tag":{
			"id": "1",
			"parity": "odd",
			"min": "1",
			"max": "3",
			"name": "SQL Storage MASTER"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "1",
		"dataLocation": "./test/test2"
	}]
},{
	"id": "14",
	"name": "Server N",
	"rts":[{
		"id": "1",
		"tag":{
			"id": "4",
			"parity": "odd",
			"min": "1",
			"max": "1",
			"name": "Elastic Search"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "1",
		"dataLocation": "./test/test2"
	},{
		"id": "2",
		"tag":{
			"id": "7",
			"parity": "odd",
			"min": "1",
			"max": "3",
			"name": "Micro Service"
		},
		"podType": "StateLess",
		"cpu": "2",
		"memory": "1",
		"count": "1",
		"dataLocation": ""
	}]
},{
	"id": "15",
	"name": "Server O",
	"rts":[{
		"id": "1",
		"tag":{
			"id": "4",
			"parity": "odd",
			"min": "1",
			"max": "1",
			"name": "Elastic Search"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "1",
		"dataLocation": "./test/test2"
	},{
		"id": "2",
		"tag":{
			"id": "5",
			"parity": "odd",
			"min": "1",
			"max": "3",
			"name": "HDFS Master Node"
		},
		"podType": "StateLess",
		"cpu": "2",
		"memory": "1",
		"count": "3",
		"dataLocation": ""
	},{
		"id": "3",
		"tag":{
			"id": "8",
			"parity": "even",
			"min": "1",
			"max": "3",
			"name": "JENKINS"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "3",
		"dataLocation": "./test/test2"
	},{
		"id": "4",
		"tag":{
			"id": "6",
			"parity": "none",
			"min": "2",
			"max": "3",
			"name": "HDFS Data Node"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "2",
		"dataLocation": "./test/test2"
	}]
},{
	"id": "16",
	"name": "Server P",
	"rts":[{
		"id": "1",
		"tag":{
			"id": "6",
			"parity": "none",
			"min": "2",
			"max": "3",
			"name": "HDFS Data Node"
		},
		"podType": "StateFull",
		"cpu": "6",
		"memory": "0.5",
		"count": "2",
		"dataLocation": "./test/test2"
	}]
},{
	"id": "17",
	"name": "Server Q",
	"rts":[{
		"id": "1",
		"tag":{
			"id": "2",
			"parity": "even",
			"min": "1",
			"max": "3",
			"name": "SQL Storage SLAVE"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "3",
		"dataLocation": "./test/test2"
	},{
		"id": "2",
		"tag":{
			"id": "3",
			"parity": "even",
			"min": "2",
			"max": "9",
			"name": "KAFKA incl. Zoo Keeper"
		},
		"podType": "StateLess",
		"cpu": "2",
		"memory": "1",
		"count": "1",
		"dataLocation": ""
	},{
		"id": "3",
		"name": "SQL MASTER",
		"tag":{
			"id": "1",
			"parity": "odd",
			"min": "1",
			"max": "3",
			"name": "SQL Storage MASTER"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "1",
		"dataLocation": "./test/test2"
	}]
},{
	"id": "18",
	"name": "Server R",
	"rts":[{
		"id": "1",
		"tag":{
			"id": "4",
			"parity": "odd",
			"min": "1",
			"max": "1",
			"name": "Elastic Search"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "1",
		"dataLocation": "./test/test2"
	},{
		"id": "2",
		"tag":{
			"id": "7",
			"parity": "odd",
			"min": "1",
			"max": "3",
			"name": "Micro Service"
		},
		"podType": "StateLess",
		"cpu": "2",
		"memory": "1",
		"count": "1",
		"dataLocation": ""
	}]
},{
	"id": "19",
	"name": "Server S",
	"rts":[{
		"id": "1",
		"tag":{
			"id": "4",
			"parity": "odd",
			"min": "1",
			"max": "1",
			"name": "Elastic Search"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "1",
		"dataLocation": "./test/test2"
	},{
		"id": "2",
		"tag":{
			"id": "5",
			"parity": "odd",
			"min": "1",
			"max": "3",
			"name": "HDFS Master Node"
		},
		"podType": "StateLess",
		"cpu": "2",
		"memory": "1",
		"count": "3",
		"dataLocation": ""
	},{
		"id": "3",
		"tag":{
			"id": "8",
			"parity": "even",
			"min": "1",
			"max": "3",
			"name": "JENKINS"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "3",
		"dataLocation": "./test/test2"
	},{
		"id": "4",
		"tag":{
			"id": "6",
			"parity": "none",
			"min": "2",
			"max": "3",
			"name": "HDFS Data Node"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "2",
		"dataLocation": "./test/test2"
	}]
},{
	"id": "20",
	"name": "Server T",
	"rts":[{
		"id": "1",
		"tag":{
			"id": "6",
			"parity": "none",
			"min": "2",
			"max": "3",
			"name": "HDFS Data Node"
		},
		"podType": "StateFull",
		"cpu": "6",
		"memory": "0.5",
		"count": "2",
		"dataLocation": "./test/test2"
	}]
},{
	"id": "21",
	"name": "Server U",
	"rts":[{
		"id": "1",
		"tag":{
			"id": "2",
			"parity": "even",
			"min": "1",
			"max": "3",
			"name": "SQL Storage SLAVE"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "3",
		"dataLocation": "./test/test2"
	},{
		"id": "2",
		"tag":{
			"id": "3",
			"parity": "even",
			"min": "2",
			"max": "9",
			"name": "KAFKA incl. Zoo Keeper"
		},
		"podType": "StateLess",
		"cpu": "2",
		"memory": "1",
		"count": "1",
		"dataLocation": ""
	},{
		"id": "3",
		"name": "SQL MASTER",
		"tag":{
			"id": "1",
			"parity": "odd",
			"min": "1",
			"max": "3",
			"name": "SQL Storage MASTER"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "1",
		"dataLocation": "./test/test2"
	}]
},{
	"id": "22",
	"name": "Server V",
	"rts":[{
		"id": "1",
		"tag":{
			"id": "4",
			"parity": "odd",
			"min": "1",
			"max": "1",
			"name": "Elastic Search"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "1",
		"dataLocation": "./test/test2"
	},{
		"id": "2",
		"tag":{
			"id": "7",
			"parity": "odd",
			"min": "1",
			"max": "3",
			"name": "Micro Service"
		},
		"podType": "StateLess",
		"cpu": "2",
		"memory": "1",
		"count": "1",
		"dataLocation": ""
	}]
},{
	"id": "23",
	"name": "Server W",
	"rts":[{
		"id": "1",
		"tag":{
			"id": "4",
			"parity": "odd",
			"min": "1",
			"max": "1",
			"name": "Elastic Search"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "1",
		"dataLocation": "./test/test2"
	},{
		"id": "2",
		"tag":{
			"id": "5",
			"parity": "odd",
			"min": "1",
			"max": "3",
			"name": "HDFS Master Node"
		},
		"podType": "StateLess",
		"cpu": "2",
		"memory": "1",
		"count": "3",
		"dataLocation": ""
	},{
		"id": "3",
		"tag":{
			"id": "8",
			"parity": "even",
			"min": "1",
			"max": "3",
			"name": "JENKINS"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "3",
		"dataLocation": "./test/test2"
	},{
		"id": "4",
		"tag":{
			"id": "6",
			"parity": "none",
			"min": "2",
			"max": "3",
			"name": "HDFS Data Node"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "2",
		"dataLocation": "./test/test2"
	}]
},{
	"id": "24",
	"name": "Server X",
	"rts":[{
		"id": "1",
		"tag":{
			"id": "6",
			"parity": "none",
			"min": "2",
			"max": "3",
			"name": "HDFS Data Node"
		},
		"podType": "StateFull",
		"cpu": "6",
		"memory": "0.5",
		"count": "2",
		"dataLocation": "./test/test2"
	}]
},{
	"id": "25",
	"name": "Server Y",
	"rts":[{
		"id": "1",
		"tag":{
			"id": "2",
			"parity": "even",
			"min": "1",
			"max": "3",
			"name": "SQL Storage SLAVE"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "3",
		"dataLocation": "./test/test2"
	},{
		"id": "2",
		"tag":{
			"id": "3",
			"parity": "even",
			"min": "2",
			"max": "9",
			"name": "KAFKA incl. Zoo Keeper"
		},
		"podType": "StateLess",
		"cpu": "2",
		"memory": "1",
		"count": "1",
		"dataLocation": ""
	},{
		"id": "3",
		"name": "SQL MASTER",
		"tag":{
			"id": "1",
			"parity": "odd",
			"min": "1",
			"max": "3",
			"name": "SQL Storage MASTER"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "1",
		"dataLocation": "./test/test2"
	}]
},{
	"id": "26",
	"name": "Server Z",
	"rts":[{
		"id": "1",
		"tag":{
			"id": "4",
			"parity": "odd",
			"min": "1",
			"max": "1",
			"name": "Elastic Search"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "1",
		"dataLocation": "./test/test2"
	},{
		"id": "2",
		"tag":{
			"id": "7",
			"parity": "odd",
			"min": "1",
			"max": "3",
			"name": "Micro Service"
		},
		"podType": "StateLess",
		"cpu": "2",
		"memory": "1",
		"count": "1",
		"dataLocation": ""
	}]
},{
	"id": "27",
	"name": "Server AA",
	"rts":[{
		"id": "1",
		"tag":{
			"id": "4",
			"parity": "odd",
			"min": "1",
			"max": "1",
			"name": "Elastic Search"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "1",
		"dataLocation": "./test/test2"
	},{
		"id": "2",
		"tag":{
			"id": "5",
			"parity": "odd",
			"min": "1",
			"max": "3",
			"name": "HDFS Master Node"
		},
		"podType": "StateLess",
		"cpu": "2",
		"memory": "1",
		"count": "3",
		"dataLocation": ""
	},{
		"id": "3",
		"tag":{
			"id": "8",
			"parity": "even",
			"min": "1",
			"max": "3",
			"name": "JENKINS"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "3",
		"dataLocation": "./test/test2"
	},{
		"id": "4",
		"tag":{
			"id": "6",
			"parity": "none",
			"min": "2",
			"max": "3",
			"name": "HDFS Data Node"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "2",
		"dataLocation": "./test/test2"
	}]
},{
	"id": "28",
	"name": "Server BB",
	"rts":[{
		"id": "1",
		"tag":{
			"id": "6",
			"parity": "none",
			"min": "2",
			"max": "3",
			"name": "HDFS Data Node"
		},
		"podType": "StateFull",
		"cpu": "6",
		"memory": "0.5",
		"count": "2",
		"dataLocation": "./test/test2"
	}]
},{
	"id": "29",
	"name": "Server CC",
	"rts":[{
		"id": "1",
		"tag":{
			"id": "2",
			"parity": "even",
			"min": "1",
			"max": "3",
			"name": "SQL Storage SLAVE"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "3",
		"dataLocation": "./test/test2"
	},{
		"id": "2",
		"tag":{
			"id": "3",
			"parity": "even",
			"min": "2",
			"max": "9",
			"name": "KAFKA incl. Zoo Keeper"
		},
		"podType": "StateLess",
		"cpu": "2",
		"memory": "1",
		"count": "1",
		"dataLocation": ""
	},{
		"id": "3",
		"name": "SQL MASTER",
		"tag":{
			"id": "1",
			"parity": "odd",
			"min": "1",
			"max": "3",
			"name": "SQL Storage MASTER"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "1",
		"dataLocation": "./test/test2"
	}]
},{
	"id": "30",
	"name": "Server DD",
	"rts":[{
		"id": "1",
		"tag":{
			"id": "4",
			"parity": "odd",
			"min": "1",
			"max": "1",
			"name": "Elastic Search"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "1",
		"dataLocation": "./test/test2"
	},{
		"id": "2",
		"tag":{
			"id": "7",
			"parity": "odd",
			"min": "1",
			"max": "3",
			"name": "Micro Service"
		},
		"podType": "StateLess",
		"cpu": "2",
		"memory": "1",
		"count": "1",
		"dataLocation": ""
	}]
},{
	"id": "31",
	"name": "Server EE",
	"rts":[{
		"id": "1",
		"tag":{
			"id": "4",
			"parity": "odd",
			"min": "1",
			"max": "1",
			"name": "Elastic Search"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "1",
		"dataLocation": "./test/test2"
	},{
		"id": "2",
		"tag":{
			"id": "5",
			"parity": "odd",
			"min": "1",
			"max": "3",
			"name": "HDFS Master Node"
		},
		"podType": "StateLess",
		"cpu": "2",
		"memory": "1",
		"count": "3",
		"dataLocation": ""
	},{
		"id": "3",
		"tag":{
			"id": "8",
			"parity": "even",
			"min": "1",
			"max": "3",
			"name": "JENKINS"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "3",
		"dataLocation": "./test/test2"
	},{
		"id": "4",
		"tag":{
			"id": "6",
			"parity": "none",
			"min": "2",
			"max": "3",
			"name": "HDFS Data Node"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "2",
		"dataLocation": "./test/test2"
	}]
},{
	"id": "32",
	"name": "Server FF",
	"rts":[{
		"id": "1",
		"tag":{
			"id": "6",
			"parity": "none",
			"min": "2",
			"max": "3",
			"name": "HDFS Data Node"
		},
		"podType": "StateFull",
		"cpu": "6",
		"memory": "0.5",
		"count": "2",
		"dataLocation": "./test/test2"
	}]
}];