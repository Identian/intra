import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { AppComponent } from './app.component';
import { FoliosExcludedComponent } from './components/folios-excluded/folios-excluded.component';
import { FoliosIncludedComponent } from './components/folios-included/folios-included.component';
import { HTTP_INTERCEPTORS, HttpClientModule } from '@angular/common/http';
import { NgbModule } from '@ng-bootstrap/ng-bootstrap';
import { UpdateIndicatorComponent } from './components/update-indicator/update-indicator.component';
import { NgxSpinnerModule } from 'ngx-spinner';
import { IPublicClientApplication, InteractionType, PublicClientApplication } from '@azure/msal-browser';
import { MSAL_GUARD_CONFIG, MSAL_INSTANCE, MsalBroadcastService, MsalGuard, MsalGuardConfiguration, MsalService } from '@azure/msal-angular';
import { loginRequest, msalConfig } from './auth-config';
import { InterceptorService } from './services/interceptor.service';

export function MSALInstanceFactory(): IPublicClientApplication {
  return new PublicClientApplication(msalConfig);
}

export function MSALGuardConfigFactory(): MsalGuardConfiguration {
  return {
    interactionType: InteractionType.Redirect,
    authRequest: loginRequest
  };
}

@NgModule({
  declarations: [
    AppComponent,
    FoliosIncludedComponent,
    FoliosExcludedComponent
  ],
  imports: [
    BrowserModule,
    HttpClientModule,
    NgbModule,
    UpdateIndicatorComponent,
    NgxSpinnerModule
  ],
  providers: [{
    provide: MSAL_INSTANCE,
    useFactory: MSALInstanceFactory
  },
  {
    provide: MSAL_GUARD_CONFIG,
    useFactory: MSALGuardConfigFactory
  },
  {
    provide: HTTP_INTERCEPTORS,
    useClass: InterceptorService,
    multi: true
  },
    MsalService,
    MsalGuard,
    MsalBroadcastService],
  bootstrap: [AppComponent]
})
export class AppModule { }
