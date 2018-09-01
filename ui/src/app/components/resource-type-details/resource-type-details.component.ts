import {
  Component,
  EventEmitter,
  Input,
  OnChanges,
  OnInit,
  Output,
  SimpleChanges,
  ViewEncapsulation
} from '@angular/core';
import {FormBuilder, FormControl, FormGroup, Validators} from '@angular/forms';

import {CommonComponent} from '../../common.component';

import {ServerDto} from '../../dto/server.dto';
import {ResourceTypeDto} from '../../dto/resourceType.dto';

import {PodTypes} from '../../constants/podTypes.constants';

@Component({
  selector: 'resource-type-details',
  encapsulation: ViewEncapsulation.None,
  templateUrl: './resource-type-details.component.html',
  styleUrls: ['./resource-type-details.component.css']
})

export class ResourceTypeDetailsComponent extends CommonComponent implements OnInit, OnChanges {
  @Input() servers: Array<ServerDto>;
  @Input() resourceType: ResourceTypeDto;
  @Input() serverId: string;
  @Input() visible: boolean;
  @Output() onSubmit = new EventEmitter();
  @Output() onClose = new EventEmitter();

  public podTypes = PodTypes;

  public sortedServers: Array<ServerDto> = [];
  public selectedServers: Array<ServerDto> = [];

  public showDataLocation: boolean = false;

  public form: FormGroup;
  public serverIdCtl: FormControl;
  public resourceTypeIdCtl: FormControl;
  public countCtl: FormControl;
  public cpuCtl: FormControl;
  public memoryCtl: FormControl;
  public podTypeCtl: FormControl;
  public dataLocationCtl: FormControl;

  constructor(private formBuilder: FormBuilder) {
    super();
  }

  ngOnInit(): void {
    this.initEmptyForm();
  }

  ngOnChanges(changes: SimpleChanges) {
    if (changes.serverId && changes.serverId.currentValue) {
      this.serverIdCtl.setValue(this.serverId);
    }
    if (changes.resourceType && changes.resourceType.currentValue) {
      let tagId = this.resourceType.tag.id;
      this.selectedServers = this.getSelectedServersByTagId(this.servers, tagId);
      this.sortedServers = this.getSortedServers(this.servers, this.selectedServers);
      this.resourceTypeIdCtl.setValue(this.resourceType.id);
      this.countCtl.setValue(this.resourceType.count);
      this.cpuCtl.setValue(this.resourceType.cpu);
      this.memoryCtl.setValue(this.resourceType.memory);
      this.podTypeCtl.setValue(this.resourceType.podType);
      this.dataLocationCtl.setValue(this.resourceType.dataLocation);
      this.showDataLocation = this.podTypeCtl.value == PodTypes.STATEFULL;
    }
  }

  public changePodType() {
    this.showDataLocation = this.podTypeCtl.value == PodTypes.STATEFULL;
    this.dataLocationCtl.reset("");
    if (this.showDataLocation) {
      this.dataLocationCtl.setValidators([Validators.required]);
      this.dataLocationCtl.updateValueAndValidity();
    }
    else {
      this.dataLocationCtl.setValidators([]);
      this.dataLocationCtl.updateValueAndValidity();
    }
  }

  public submitRsourceTypeDialog() {
    let selectedServerIds = this.selectedServers.map(s => s.id).join(",");
    let serversListRequest = {
      "tagId": this.resourceType.tag.id,
      "ids": selectedServerIds
    }
    let resourceTypeRequest = {
      "serverId": this.serverIdCtl.value,
      "resourceTypeId": this.resourceTypeIdCtl.value,
      "count": this.countCtl.value,
      "cpu": this.cpuCtl.value,
      "memory": this.memoryCtl.value,
      "podType": this.podTypeCtl.value,
      "dataLocation": this.dataLocationCtl.value,
      "tagId": this.resourceType.tag.id
    }
    let request = {
      serversList: serversListRequest,
      resourceType: resourceTypeRequest
    }
    this.onSubmit.emit(request);
  }

  public closeRsourceTypeDialog() {
    this.onClose.emit();
  }

  private initEmptyForm() {
    this.serverIdCtl = new FormControl("", Validators.required);
    this.resourceTypeIdCtl = new FormControl("", Validators.required);
    this.countCtl = new FormControl("", Validators.required);
    this.cpuCtl = new FormControl("", Validators.required);
    this.memoryCtl = new FormControl("", Validators.required);
    this.podTypeCtl = new FormControl("", Validators.required);
    this.dataLocationCtl = new FormControl("");
    this.form = this.formBuilder.group({
      "serverId": this.serverIdCtl,
      "resourceTypeId": this.resourceTypeIdCtl,
      "count": this.countCtl,
      "cpu": this.cpuCtl,
      "memory": this.memoryCtl,
      "podType": this.podTypeCtl,
      "dataLocation": this.dataLocationCtl
    });
  }
}
