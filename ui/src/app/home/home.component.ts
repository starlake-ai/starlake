import { Component, OnInit } from '@angular/core';
// import { Router } from '@angular/router';
// import { Message } from 'primeng/primeng';

import { ResourceTypesService } from '../services/resourceTypes.service';
import { TagDto } from '../dto/tags.dto';

import * as $ from 'jquery';

@Component({
	templateUrl: './home.component.html',
	styleUrls: ['./home.component.css']
})
export class HomeComponent implements OnInit {

	public resourceTypes: Array<TagDto>;
	// public msgs: Array<Message>;

	constructor(private resourceTypesService: ResourceTypesService) {
		// this.msgs = [];
	}

	ngOnInit(): void {
		this.resourceTypesService.getAllResourceTypes().subscribe(
			result => this.resourceTypes = result,
			err => {
				this.resourceTypes = [{"id": "1","parity": "1","min": 1,"max": 3,"name": "SQL Storage MASTER"},{"id": "2","parity": "1","min": 1,"max": 3,"name": "SQL Storage SLAVE"},{"id": "3","parity": "2","min": 2,"max": 9,"name": "KAFKA incl. Zoo Keeper"},{"id": "4","parity": "1","min": 1,"max": 1,"name": "Elastic Search"},{"id": "5","parity": "1","min": 1,"max": 3,"name": "HDFS Master Node"},{"id": "6","parity": "2","min": 2,"max": 3,"name": "HDFS Data Node"},{"id": "7","parity": "1","min": 1,"max": 3,"name": "Micro Service"},{"id": "8","parity": "1","min": 1,"max": 3,"name": "JENKINS"},{"id": "9","parity": "1","min": 1,"max": 3,"name": "DOKER Rep."},{"id": "10","parity": "1","min": 1,"max": 3,"name": "Artifactory"},{"id": "11","parity": "1","min": 1,"max": 3,"name": "Public Slave"}];
			} // TO BE DELETED
		);
	}
}