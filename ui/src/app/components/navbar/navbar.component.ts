import {Component, OnInit} from '@angular/core';

// import { AuthGuard } from '../guards/auth.guard';
// import { LoginService } from '../services/login.service';
// import { SharedService } from '../services/shared.service';
// import { CurrentUtilisateurService } from '../services/currentUtilisateur.service';

@Component({
  selector: 'navbar',
  templateUrl: './navbar.component.html',
  styleUrls: ['./navbar.component.css']
})

export class NavBarComponent implements OnInit {

  // private isConnected: boolean = false;
  // private canExplore: boolean = false;
  // private canManage: boolean = false;
  // private canClassify: boolean = false;
  // private roles: any = null;

  constructor(/*private sharedService: SharedService,
				private loginService: LoginService,
				private router: Router,
				private authGuard: AuthGuard,
				private currentUserService: CurrentUtilisateurService*/) {
  }

  ngOnInit(): void {
    // this.roles = this.currentUserService.currentUserRoles();
    // this.canExplore = this.roles.includes('ROLE_DXP') ||
    // this.roles.includes('ROLE_LLV') ||
    // this.roles.includes('ROLE_MLV') ||
    // this.roles.includes('ROLE_A') ||
    // this.roles.includes('ROLE_DP') ||
    // this.roles.includes('ROLE_ADMIN');

    // this.canManage = this.roles.includes('ROLE_DXP') ||
    // this.roles.includes('ROLE_LLV') ||
    // this.roles.includes('ROLE_MLV') ||
    // this.roles.includes('ROLE_A') ||
    // this.roles.includes('ROLE_DP') ||
    // this.roles.includes('ROLE_ADMIN');

    // this.canClassify = this.roles.includes('ROLE_DXP') ||
    // this.roles.includes('ROLE_LLV') ||
    // this.roles.includes('ROLE_MLV') ||
    // this.roles.includes('ROLE_A') ||
    // this.roles.includes('ROLE_DP') ||
    // this.roles.includes('ROLE_ADMIN');

    // this.sharedService.dataChange.subscribe(data => {
    // this.isConnected = data.userConnected;
    // });
  }
}
