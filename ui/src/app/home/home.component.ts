import { Component, OnInit, OnDestroy, AfterViewInit } from '@angular/core';
import { FormGroup, FormControl, Validators, FormBuilder } from '@angular/forms';

import { CommonComponent } from '../common.component';

import { ResourceTypesService } from '../services/resourceTypes.service';
import { ServersService } from '../services/servers.service';
import { JsonService } from '../services/json.service'; // TO BE DELETED
import { TagDto } from '../dto/tag.dto';
import { ServerDto } from '../dto/server.dto';
import { ResourceTypeDto } from '../dto/resourceType.dto';

import * as $ from 'jquery';

@Component({
	templateUrl: './home.component.html',
	styleUrls: ['./home.component.css']
})
export class HomeComponent extends CommonComponent implements OnInit, OnDestroy, AfterViewInit {
	private resourceTypesSubscription: any = null;
	public resourceTypes: Array<TagDto>;

	private serversSubscription: any = null;
	public servers: Array<ServerDto> = [];

	public draggedItem: TagDto;
	public selectedResourceType: ResourceTypeDto;
	public selectedServerId: string;

	public showAddServer:boolean = false;
	public newServerForm: FormGroup;
	public newServerNameCtl: FormControl;
	
	public errorMessage:string = "";
	public showErrorDialog:boolean = false;

	public showResourceTypeDialog:boolean = false;

	public selectedTagId:string = "";
	public showListServers:boolean = false;

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
				this.getAllServers();
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
	public dragStart(event, tag: TagDto) {
		this.draggedItem = tag;
	}

	public dragEnd(event) {
		this.draggedItem = null;
		$("td.highlight").removeClass("highlight");
		$("td.disabled").removeClass("disabled");
	}

	public dragEnter(event) {
		if($(event.target).prop("tagName").toLowerCase() != "td")
			return false;
		$("td.highlight").removeClass("highlight");
		$("td.disabled").removeClass("disabled");
		let serverId = $(event.target).data("server-id");
		let server = this.servers.find(s => s.id == serverId);
		if(this.draggedItem){
			if(server.rts.filter(r => r.tag.id == this.draggedItem.id).length == 0){
				$(event.target).closest("td").addClass("highlight");
			}
			else{
				$(event.target).closest("td").addClass("disabled");
			}
		}
	}

	public dragLeave(event) {
	}

	public drop(event) {
		if(this.draggedItem) {
			$("td.highlight").removeClass("highlight");
			$("td.disabled").removeClass("disabled");
			let serverId = $(event.target).closest("td").data("server-id");
			let server = this.servers.find(s => s.id == serverId);
			if(server.rts.filter(r => r.tag.id == this.draggedItem.id).length > 0){
				this.errorMessage = "Tag " + this.draggedItem.id + " already exist in server " + server.name;
				this.showErrorDialog = true;
				return false;
			}
			let params:any = {
				resourceTypeId: this.draggedItem.id,
				podType: "STATELESS",
				cpu: "0",
				memory: "0",
				count: "0",
				dataLocation: ""
			}
			this.showMask();
			this.serversService.addResourceType(serverId, params).subscribe(
				data => {
					this.hideMask();
				},
				err => {
					this.servers = this.jsonService.addResourceType(serverId, params);
					this.hideMask();
				}
			);
			// let rt  = {
				// podType: "STATELESS",
				// cpu: "",
				// memory: "",
				// count: "",
				// dataLocation: "",
				// tag: this.draggedItem
			// }
			// this.openResourceType(rt, serverId);
			// this.draggedItem = null;
		}
	}

	public canDropItem(server){
		if(this.draggedItem){
			if(server.rts.filter(r => r.tag.id == this.draggedItem.id).length == 0)
				return true;
		}
		return false;
	}

	public canNotDropItem(server){
		if(this.draggedItem){
			if(server.rts.filter(r => r.tag.id == this.draggedItem.id).length > 0)
				return true;
		}
		return false;
	}

	public openResourceType(resourceType, serverId){
		this.selectedResourceType = resourceType;
		this.selectedServerId = serverId;
		this.showResourceTypeDialog = true;
	}

	public deleteResourceType(resourceTypeId, serverId){
		this.showMask();
		this.serversService.deleteResourceType(serverId, resourceTypeId).subscribe(
			data => {
				this.hideMask();
			},
			err => {
				this.servers = this.jsonService.deleteResourceType(serverId, resourceTypeId);
				this.hideMask();
			}
		);
	}

	public closeResourceType(){
		this.showResourceTypeDialog = false;
	}

	public saveResourceType(request:any){
		// TODO CALL SERVICE
		console.log(request);
		this.closeResourceType();
	}

// Servers List
	public openListServers(id){
		this.selectedTagId = id;
		this.showListServers = true;
	}

	public closeListServers(){
		this.showListServers = false;
		$(".ui-dialog-content").scrollTop(0);
	}

	public saveListServers(request:any){
		// TODO CALL SERVICE
		console.log(request);
		this.closeListServers();
	}

// Error Dialogue
	public closeErrorDialog(){
		this.showErrorDialog = false;
		this.errorMessage = "";
	}
}