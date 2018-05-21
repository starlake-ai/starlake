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
}];