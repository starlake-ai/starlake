// Angular
import { APP_INITIALIZER, NgModule } from '@angular/core';
import { Routes } from '@angular/router';
import { CommonModule } from '@angular/common';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { HttpModule, Http, RequestOptions } from '@angular/http';
import { RouterModule } from '@angular/router';
import { BrowserModule } from '@angular/platform-browser';

// PrimeNg
import {DragDropModule} from 'primeng/dragdrop';
import {DataTableModule} from 'primeng/datatable';
import {PanelModule} from 'primeng/panel';

// Configuration
import { AppConfiguration } from '../environments/app.configuration';

// Services
import { ResourceTypesService } from './services/resourceTypes.service';

// Components
import { AppComponent } from './app.component';
import { NavBarComponent } from './navbar/navbar.component';
import { HomeComponent } from './home/home.component';

export function configServiceFactory(config: AppConfiguration) {
	return () => config.load();
}

// Routes
import { AppRoutes } from './app.routes';

@NgModule({
	declarations: [
		AppComponent,
		NavBarComponent,
		HomeComponent
	],
	imports: [
		BrowserModule,
		FormsModule,
		ReactiveFormsModule,
		CommonModule,
		HttpModule,
		RouterModule.forRoot(AppRoutes, {useHash: false}),
		DragDropModule,
		DataTableModule,
		PanelModule
	],
	providers: [
		AppConfiguration,
		{
			provide: APP_INITIALIZER,
			useFactory: configServiceFactory,
			deps: [AppConfiguration],
			multi: true
		},
		ResourceTypesService
	],
	bootstrap: [AppComponent]
})
export class AppModule { }
