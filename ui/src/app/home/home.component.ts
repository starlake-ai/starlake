import { Component, OnInit } from '@angular/core';
// import { Router } from '@angular/router';
// import { Message } from 'primeng/primeng';

import {DragDropModule} from 'primeng/dragdrop';
import {DataTableModule} from 'primeng/datatable';
import {PanelModule} from 'primeng/panel';

import { ResourceTypesService } from '../services/resourceTypes.service';
import { TagDto } from '../dto/tag.dto';

import * as $ from 'jquery';

@Component({
	templateUrl: './home.component.html',
	styleUrls: ['./home.component.css']
})
export class HomeComponent implements OnInit {

	public resourceTypes: Array<TagDto>;
	// public msgs: Array<Message>;
    
    draggedItem: TagDto;
	
	constructor(private resourceTypesService: ResourceTypesService) {
		// this.msgs = [];
	}

	ngOnInit(): void {
		this.resourceTypesService.getAllResourceTypes().subscribe(
			result => this.resourceTypes = result,
			err => {
				this.resourceTypes = [{"id": "1","parity": "odd","min": "1","max": "3","name": "SQL Storage MASTER"},{"id": "2","parity": "even","min": "1","max": "3","name": "SQL Storage SLAVE"},{"id": "3","parity": "even","min": "2","max": "9","name": "KAFKA incl. Zoo Keeper"},{"id": "4","parity": "odd","min": "1","max": "1","name": "Elastic Search"},{"id": "5","parity": "odd","min": "1","max": "3","name": "HDFS Master Node"},{"id": "6","parity": "none","min": "2","max": "3","name": "HDFS Data Node"},{"id": "7","parity": "odd","min": "1","max": "3","name": "Micro Service"},{"id": "8","parity": "even","min": "1","max": "3","name": "JENKINS"},{"id": "9","parity": "even","min": "1","max": "3","name": "DOKER Rep."},{"id": "10","parity": "even","min": "1","max": "3","name": "Artifactory"},{"id": "11","parity": "none","min": "1","max": "3","name": "Public Slave"}];
			} // TO BE DELETED
		);
	}
	
	dragStart(event,tag: TagDto) {
        this.draggedItem = tag;
    }
    
    drop(event) {
        if(this.draggedItem) {
			console.log(this.draggedItem);
            this.draggedItem = null;
        }
    }
    
    dragEnd(event) {
			console.log(this.draggedItem);
        this.draggedItem = null;
    }
}