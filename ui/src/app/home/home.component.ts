import { Component, OnInit, OnDestroy, AfterViewInit } from '@angular/core';
import { FormGroup, FormControl, Validators, FormBuilder } from '@angular/forms';

import { CommonComponent } from '../common.component';

import {DragDropModule} from 'primeng/dragdrop';
import {TableModule} from 'primeng/table';
import {PanelModule} from 'primeng/panel';
import {DialogModule} from 'primeng/dialog';
import {TabViewModule} from 'primeng/tabview';

import { ResourceTypesService } from '../services/resourceTypes.service';
import { ServersService } from '../services/servers.service';
import { JsonService } from '../services/json.service'; // TO BE DELETED
import { TagDto } from '../dto/tag.dto';
import { ServerDto } from '../dto/server.dto';

import * as $ from 'jquery';

@Component({
	templateUrl: './home.component.html',
	styleUrls: ['./home.component.css']
})
export class HomeComponent extends CommonComponent implements OnInit, OnDestroy, AfterViewInit {
	private resourceTypesSubscription: any = null;
	public resourceTypes: Array<TagDto>;

	private serversSubscription: any = null;
	public servers: Array<ServerDto>;

	public draggedItem: TagDto;

	public showAddServer:boolean = false;
	public newServerForm: FormGroup;
	public newServerNameCtl: FormControl;

	public showResourceTypeDialog:boolean = false;
	public resourceTypeForm: FormGroup;
	public resourceTypeIdCtl: FormControl;
	public resourceTypeCountCtl: FormControl;
	public resourceTypeCpuCtl: FormControl;
	public resourceTypeMemoryCtl: FormControl;
	public resourceTypePodTypeCtl: FormControl;
	public resourceTypeDataLocationCtl: FormControl;

	constructor(
		private formBuilder: FormBuilder,
		private resourceTypesService: ResourceTypesService,
		private serversService: ServersService,
		private jsonService: JsonService // TO BE DELETED
	) {
		super();
	}

	ngOnInit(): void {
		this.showMask();
		this.getAllResourceTypes();
		this.getAllServers();
		this.initEmptyNewServerForm();
		this.initEmptyResourceTypeForm();
	}

	ngAfterViewInit() {}

	ngOnDestroy() {
		this.resourceTypesSubscription.unsubscribe();
		this.serversSubscription.unsubscribe();
	}

	private getAllResourceTypes(){
		this.resourceTypesSubscription = this.resourceTypesService.getAllResourceTypes().subscribe(
			result => {
				this.resourceTypes = result;
			},
			err => {
				this.jsonService.getStaticResourceTypes().then(RT => this.resourceTypes = RT);
			}
		);
	}

	private getAllServers(){
		this.serversSubscription = this.serversService.getAllServers().subscribe(
			result => {
				this.servers = result;
				this.hideMask();
			},
			err => {
				this.jsonService.getStaticServers().then( RTS => this.servers = RTS );
				this.hideMask();
			}
		);
	}
// Server
	private initEmptyNewServerForm(){
		this.newServerNameCtl = new FormControl("", Validators.required);
		this.newServerForm = this.formBuilder.group({
			"name": this.newServerNameCtl
		});
	}

	public addNewServer(){
		if(!this.newServerForm.valid){
			this.newServerNameCtl.markAsDirty();
			return;
		}
		let params:any = {
			name: this.newServerNameCtl.value
		};
		this.closeNewServerForm();
		this.showMask();
		this.serversService.addNewServer(params).subscribe(
			data => {
				this.hideMask();
			},
			err => {
				this.servers = this.jsonService.addNewServer(params);
				this.hideMask();
			}
		);
	}

	public closeNewServerForm(){
		this.showAddServer = false;
		this.newServerNameCtl.reset("");
	}

// Resource Type
	public dragStart(event,tag: TagDto) {
		this.draggedItem = tag;
	}

	public drop(event) {
		if(this.draggedItem) {
			// let serverId = $(event.target).data("server-id");
			// let params:any = {
				// resourceTypeId: this.draggedItem.id,
				// podType: "STATELESS",
				// cpu: "0",
				// memory: "0",
				// count: "0",
				// dataLocation: ""
			// }
			// this.showMask();
			// this.serversService.addResourceType(serverId, params).subscribe(
				// data => {
					// this.hideMask();
				// },
				// err => {
					// this.servers = this.jsonService.addResourceType(serverId, params);
					// this.hideMask();
				// }
			// );
			this.showResourceTypeDialog = true;
			this.resourceTypeIdCtl.setValue(this.draggedItem.id);
			this.draggedItem = null;
		}
	}

	public dragEnd(event) {
		this.draggedItem = null;
	}

	private initEmptyResourceTypeForm(){
		this.resourceTypeIdCtl = new FormControl("", Validators.required);
		this.resourceTypeCountCtl = new FormControl("", Validators.required);
		this.resourceTypeCpuCtl = new FormControl("", Validators.required);
		this.resourceTypeMemoryCtl = new FormControl("", Validators.required);
		this.resourceTypePodTypeCtl = new FormControl("", Validators.required);
		this.resourceTypeDataLocationCtl = new FormControl("");
		this.resourceTypeForm = this.formBuilder.group({
			"id": this.resourceTypeIdCtl,
			"count": this.resourceTypeCountCtl,
			"cpu": this.resourceTypeCpuCtl,
			"memory": this.resourceTypeMemoryCtl,
			"podType": this.resourceTypePodTypeCtl,
			"dataLocation": this.resourceTypeDataLocationCtl
		});
	}

	public closeRsourceTypeDialog(){
		this.showResourceTypeDialog = false;
	}
}