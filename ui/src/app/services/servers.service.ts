import {Injectable} from '@angular/core';
import {Http} from '@angular/http';
import {AbstractServerService} from './abstract.service';

// import {AuthHttp} from 'angular2-jwt';

@Injectable()
export class ServersService extends AbstractServerService {

  constructor(http: Http/*, authHttp: AuthHttp*/) {
    super(http/*, authHttp*/)
  }

  getAllServers() {
    return this.get(`servers`);
  }

  addNewServer(params: any) {
    return this.post(`servers`, params);
  }

  addResourceType(id: string, params: any) {
    return this.post(`servers/${id}/resourceType`, params);
  }

  deleteResourceType(id: string, resourceTypeId: string) {
    return this.delete(`servers/${id}/resourceType/${resourceTypeId}`);
  }
}
