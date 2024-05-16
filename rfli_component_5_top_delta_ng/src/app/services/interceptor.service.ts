import { HttpEvent, HttpHandler, HttpInterceptor, HttpRequest } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { MsalService } from "@azure/msal-angular";
import { delay, finalize, from, lastValueFrom, Observable } from "rxjs";
import { msalConfig } from "../auth-config";

 

@Injectable({

    providedIn: 'root'

})

 

export class InterceptorService implements HttpInterceptor {

 

    constructor(private msalService: MsalService) {}

    intercept(req: HttpRequest<any>, next: HttpHandler): Observable<HttpEvent<any>> {
        return from(this.handle(req, next))
    }

 

    async handle(req: HttpRequest<any>, next: HttpHandler) {

      
        if (this.validateTokenExp()) {

            await this.refreshToken()

            let token = JSON.parse(sessionStorage.getItem('token')!).accessToken
            req = req.clone({
                //headers: req.headers.set('Authorization', `${token}`)
                 setHeaders:{
                     'Content-Type': 'application/json; charset=utf-8',
                     Accept: 'application/json',
                     Authorization: `${token}` 
                 }
            });

            return await lastValueFrom(next.handle(req));

        }

 

        let token = JSON.parse(sessionStorage.getItem('token')!).accessToken

 

        req = req.clone({

            //headers: req.headers.set('Authorization', `${token}`)
             setHeaders:{
                 'Content-Type': 'application/json; charset=utf-8',
                 Accept: 'application/json',
                 Authorization: `${token}` 
             }

        });

        return await lastValueFrom(next.handle(req));
    }


    async refreshToken() {
      const silentRequest = {
          scopes: [msalConfig.auth.clientId + "/.default"],
          account: this.msalService.instance.getActiveAccount()!
      }

      await this.msalService.instance.acquireTokenSilent(silentRequest).then(response => {
          sessionStorage.setItem('token', JSON.stringify(response))
      })
  }

  validateTokenExp() {
    const tokenExpirateOn: any = JSON.parse(sessionStorage.getItem('token')!)
    const forceRefresh = (new Date(tokenExpirateOn.expiresOn) < new Date());
    if (forceRefresh) {
        return true
    }
    return false

}

}