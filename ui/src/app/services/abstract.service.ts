import {Headers, Http, RequestOptions, Response} from '@angular/http';
import {Observable} from 'rxjs';
import {catchError, map} from 'rxjs/operators';
// import {AuthHttp} from 'angular2-jwt';
import {AppConfiguration} from '../../environments/app.configuration';


/**
 * Classe de base des services qui appelle le Back
 */
export class AbstractServerService {

  JSON_HEADERS = new Headers({'Content-Type': 'application/json'});

  constructor(private http: Http/*, private authHttp: AuthHttp*/) {
  }

  headers(name: string, value: string) {
    let newHeader = new Headers(this.JSON_HEADERS);
    newHeader.append(name, value);
    return newHeader;
  }

  post(url: string, param: any = null, options: RequestOptions = new RequestOptions({headers: this.JSON_HEADERS})) {
    let body = (param != null) ? JSON.stringify(param) : "";
    return this.http.post(AppConfiguration.BACKEND_URL + url, body, options).pipe(map(this.extractData), catchError(this.handleError));
  }

  // postUsingAuth(url: string, param: any = null, options: RequestOptions = new RequestOptions({headers: this.JSON_HEADERS})) {
  // let body = (param != null) ? JSON.stringify(param) : "";
  // return this.authHttp.post(AppConfiguration.BACKEND_URL + url, body, options).pipe(map(this.extractData), catchError(this.handleError));
  // }

  // postFormData(url: string, formData: FormData = new FormData(), options: RequestOptions = new RequestOptions({})) {
  // return this.authHttp.post(AppConfiguration.BACKEND_URL + url, formData, options).pipe(map(this.extractData), catchError(this.handleError));
  // }

  put(url: string, param: any = null, options: RequestOptions = new RequestOptions({headers: this.JSON_HEADERS})) {
    let body = (param != null) ? JSON.stringify(param) : "";
    return this.http.put(AppConfiguration.BACKEND_URL + url, body, options).pipe(map(this.extractData), catchError(this.handleError));
  }

  // putUsingAuth(url: string, param: any = null, options: RequestOptions = new RequestOptions({headers: this.JSON_HEADERS})) {
  // let body = (param != null) ? JSON.stringify(param) : "";
  // return this.authHttp.put(AppConfiguration.BACKEND_URL + url, body, options).pipe(map(this.extractData), catchError(this.handleError));
  // }

  get(url: string, query: URLSearchParams = null) {
    let options = new RequestOptions({headers: this.JSON_HEADERS, params: query});
    return this.http.get(AppConfiguration.BACKEND_URL + url, options).pipe(map(this.extractData), catchError(this.handleError));
  }

  // getUsingAuth(url: string, query: URLSearchParams = null) {
  // let options = new RequestOptions({headers: this.JSON_HEADERS, params: query});
  // return this.authHttp.get(AppConfiguration.BACKEND_URL + url, options).pipe(map(this.extractData), catchError(this.handleError));
  // }

  delete(url: string, query: URLSearchParams = null) {
    let options = new RequestOptions({headers: this.JSON_HEADERS, params: query});
    return this.http.delete(AppConfiguration.BACKEND_URL + url, options).pipe(map(this.extractData), catchError(this.handleError));
  }

  // deleteUsingAuth(url: string, query: URLSearchParams = null) {
  // let options = new RequestOptions({headers: this.JSON_HEADERS, params: query});
  // return this.authHttp.delete(AppConfiguration.BACKEND_URL + url, options).pipe(map(this.extractData), catchError(this.handleError));
  // }

  private extractData(response: Response) {
    try {
      if (response != null) return response.json();
    }
    catch (e) {
    }
    return null;
  }

  private handleError(error: any) {
    console.error('An error occurred', error);
    return Observable.throw(error);
  }
}
