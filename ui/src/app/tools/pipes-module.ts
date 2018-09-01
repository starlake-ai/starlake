import {NgModule} from '@angular/core';

import {SortByPipe} from './sort-by.pipe'

@NgModule({
  declarations: [
    SortByPipe
  ],
  exports: [
    SortByPipe
  ],
  imports: [],
  providers: []
})

export class PipesModule {
}
