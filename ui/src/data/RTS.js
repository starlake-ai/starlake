var staticRTS = [{
	"id": "1",
	"name": "Server A",
	"RTS":[{
		"id": "1",
		"name": "SQL Storage SLAVE",
		"rules":{
			"id": "2",
			"parity": "even",
			"min": "1",
			"max": "3"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "2",
		"dataLocation": "./test/test2"
	},{
		"id": "2",
		"name": "KAFKA",
		"rules":{
			"id": "3",
			"parity": "even",
			"min": "2",
			"max": "9"
		},
		"podType": "StateLess",
		"cpu": "2",
		"memory": "1",
		"count": "2",
		"dataLocation": ""
	},{
		"id": "1",
		"name": "SQL Storage MASTER",
		"rules":{
			"id": "1",
			"parity": "odd",
			"min": "1",
			"max": "3"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "2",
		"dataLocation": "./test/test2"
	}]
},{
	"id": "2",
	"name": "Server B",
	"RTS":[{
		"id": "1",
		"name": "Elastic Search",
		"rules":{
			"id": "4",
			"parity": "odd",
			"min": "1",
			"max": "1"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "2",
		"dataLocation": "./test/test2"
	},{
		"id": "2",
		"name": "Micro Service",
		"rules":{
			"id": "7",
			"parity": "odd",
			"min": "1",
			"max": "3"
		},
		"podType": "StateLess",
		"cpu": "2",
		"memory": "1",
		"count": "2",
		"dataLocation": ""
	}]
},{
	"id": "3",
	"name": "Server C",
	"RTS":[{
		"id": "1",
		"name": "Elastic Search",
		"rules":{
			"id": "4",
			"parity": "odd",
			"min": "1",
			"max": "1"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "2",
		"dataLocation": "./test/test2"
	},{
		"id": "2",
		"name": "HDFS Master Node",
		"rules":{
			"id": "5",
			"parity": "odd",
			"min": "1",
			"max": "3"
		},
		"podType": "StateLess",
		"cpu": "2",
		"memory": "1",
		"count": "2",
		"dataLocation": ""
	},{
		"id": "3",
		"name": "Elastic Search",
		"rules":{
			"id": "4",
			"parity": "odd",
			"min": "1",
			"max": "1"
		},
		"podType": "StateFull",
		"cpu": "2",
		"memory": "1",
		"count": "2",
		"dataLocation": "./test/test2"
	},{
		"id": "4",
		"name": "HDFS Data Node",
		"rules":{
			"id": "6",
			"parity": "none",
			"min": "2",
			"max": "3"
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
	"RTS":[{
		"id": "1",
		"name": "HDFS Data Node",
		"rules":{
			"id": "6",
			"parity": "none",
			"min": "2",
			"max": "3"
		},
		"podType": "StateFull",
		"cpu": "6",
		"memory": "0.5",
		"count": "2",
		"dataLocation": "./test/test2"
	}]
}];