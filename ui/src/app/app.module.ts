// Angular
import { APP_INITIALIZER, NgModule } from '@angular/core';
import { Routes } from '@angular/router';
import { CommonModule } from '@angular/common';
import { HttpClientModule } from '@angular/common/http'; // TO BE DELETED
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { HttpModule, Http, RequestOptions } from '@angular/http';
import { RouterModule } from '@angular/router';
import { BrowserModule } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';

// PrimeNg
import {DragDropModule} from 'primeng/dragdrop';
import {TableModule} from 'primeng/table';
import {DialogModule} from 'primeng/dialog';
import {TabViewModule} from 'primeng/tabview';
import {TooltipModule} from 'primeng/tooltip';

// Configuration
import { AppConfiguration } from '../environments/app.configuration';

// Services
import { JsonService } from './services/json.service'; // TO BE DELETED
import { ResourceTypesService } from './services/resourceTypes.service';
import { ServersService } from './services/servers.service';

//Tools
import {PipesModule} from './tools/pipes-module';

// Components
import { AppComponent } from './app.component';
import { HomeComponent } from './home/home.component';
import { NavBarComponent } from './components/navbar/navbar.component';
import { ListServersComponent } from './components/list-servers/list-servers.component';
import { ResourceTypeDetailsComponent } from './components/resource-type-details/resource-type-details.component';

export function configServiceFactory(config: AppConfiguration) {
	return () => config.load();
}

// Routes
import { AppRoutes } from './app.routes';

@NgModule({
	declarations: [
		AppComponent,
		HomeComponent,
		NavBarComponent,
		ListServersComponent,
		ResourceTypeDetailsComponent
	],
	imports: [
		BrowserModule,
		BrowserAnimationsModule,
		FormsModule,
		ReactiveFormsModule,
		CommonModule,
		HttpModule,
		HttpClientModule, // TO BE DELETED
		RouterModule.forRoot(AppRoutes, {useHash: false}),
		PipesModule,
	// PrimeNg
		DragDropModule,
		TableModule,
		DialogModule,
		TabViewModule,
		TooltipModule
	],
	providers: [
		AppConfiguration,
		{
			provide: APP_INITIALIZER,
			useFactory: configServiceFactory,
			deps: [AppConfiguration],
			multi: true
		},
		JsonService, // TO BE DELETED
		ResourceTypesService,
		ServersService
	],
	bootstrap: [AppComponent]
})
export class AppModule { }
