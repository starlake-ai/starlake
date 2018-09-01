import {Http, Response} from '@angular/http';
import {Injectable} from '@angular/core';
import {Observable} from 'rxjs';
import {catchError, map} from 'rxjs/operators';

/**
 * @author yassir
 * configuration de l'application aprés le bootstrap
 */
@Injectable()
export class AppConfiguration {
  public static BACKEND_URL: string;
  public static KEY_LOCAL_STORAGE_TOKEN: string = 'token';
  public static KEY_LOCAL_STORAGE_TOKEN_REFERECH: string = 'refreshToken';
  public static KEY_LOCAL_STORAGE_USER_PROXY: string = 'userProxy';

  constructor(private http: Http) {
  }

  load() {
    return new Promise((resolve, reject) => {
      this.http.get('/assets/config/env.json')
        .pipe(map(this.extractData), catchError(this.handleError))
        .subscribe((env_data) => {
          this.http.get('/assets/config/config-' + env_data.env + '.json')
            .pipe(map(this.extractData), catchError(this.handleError))
            .subscribe((json) => {
              AppConfiguration.BACKEND_URL = json.server;
              resolve(true);
            });
        });
    });
  }

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
