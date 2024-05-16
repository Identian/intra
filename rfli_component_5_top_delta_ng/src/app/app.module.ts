import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { BrowserAnimationsModule } from "@angular/platform-browser/animations";
import { FormsModule } from '@angular/forms';
import { AppRoutingModule } from './app-routing.module';
import { AppComponent } from './app.component';
import { EmptyRouteComponent } from './empty-route/empty-route.component';
import { CategoryCardComponent } from './components/category-card/category-card.component';
import { NgbModule } from '@ng-bootstrap/ng-bootstrap';
import { CommonModule } from '@angular/common';
import { IPublicClientApplication, InteractionType, PublicClientApplication } from '@azure/msal-browser';
import { loginRequest, msalConfig } from './auth-config';
import { MSAL_GUARD_CONFIG, MSAL_INSTANCE, MsalBroadcastService, MsalGuard, MsalGuardConfiguration, MsalService } from '@azure/msal-angular';
import { HTTP_INTERCEPTORS, HttpClientModule } from '@angular/common/http';
import { InterceptorService } from './services/interceptor.service';
import { UpdateIndicatorComponent } from './components/update-indicator/update-indicator.component';
import { NgxSpinnerModule } from 'ngx-spinner';

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
    EmptyRouteComponent,
    CategoryCardComponent
  ],
  imports: [
    BrowserModule,
    AppRoutingModule,
    NgbModule, 
    HttpClientModule,
    CommonModule,
    FormsModule,
    UpdateIndicatorComponent,
    BrowserAnimationsModule,
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
