import {Injectable} from '@angular/core';
import {Http} from '@angular/http';
import {AbstractServerService} from './abstract.service';

// import {AuthHttp} from 'angular2-jwt';

@Injectable()
export class ResourceTypesService extends AbstractServerService {

  constructor(http: Http/*, authHttp: AuthHttp*/) {
    super(http/*, authHttp*/)
  }

  getAllResourceTypes() {
    return this.get("resourceTypes");
  }
}
