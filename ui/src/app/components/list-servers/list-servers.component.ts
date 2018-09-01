import {Component, EventEmitter, Input, OnChanges, Output, SimpleChanges, ViewEncapsulation} from '@angular/core';

import {CommonComponent} from '../../common.component';

import {ServerDto} from '../../dto/server.dto';

@Component({
  selector: 'list-servers-dialog',
  encapsulation: ViewEncapsulation.None,
  templateUrl: './list-servers.component.html',
  styleUrls: ['./list-servers.component.css']
})

export class ListServersComponent extends CommonComponent implements OnChanges {
  @Input() servers: Array<ServerDto>;
  @Input() tagId: string;
  @Input() visible: boolean;
  @Output() onSubmit = new EventEmitter();
  @Output() onClose = new EventEmitter();

  public sortedServers: Array<ServerDto> = [];
  public selectedServers: Array<ServerDto> = [];

  constructor() {
    super();
  }

  ngOnChanges(changes: SimpleChanges) {
    if (changes.tagId) {
      this.selectedServers = this.getSelectedServersByTagId(this.servers, this.tagId);
      this.sortedServers = this.getSortedServers(this.servers, this.selectedServers);
    }
  }

  public submitServersDialog() {
    let selectedServerIds = this.selectedServers.map(s => s.id).join(",");
    let request = {
      "tagId": this.tagId,
      "ids": selectedServerIds
    }
    this.onSubmit.emit(request);
  }

  public closeServersDialog() {
    this.onClose.emit();
  }
}
