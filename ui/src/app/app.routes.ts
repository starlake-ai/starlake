import {Route} from '@angular/router';

import {HomeComponent} from './home/home.component';

export const AppRoutes: Route[] = [
  {
    path: '',
    canActivate: [],
    component: HomeComponent
  }
];
