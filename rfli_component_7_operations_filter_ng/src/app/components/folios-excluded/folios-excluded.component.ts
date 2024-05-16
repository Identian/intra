import { Component, OnInit } from '@angular/core';
import { NgxSpinnerService } from 'ngx-spinner';
import { IFolio } from 'src/app/interfaces/Folios';
import { FolioService } from 'src/app/services/folio.service';

@Component({
  selector: 'precia-folios-excluded',
  templateUrl: './folios-excluded.component.html',
  styleUrls: ['./folios-excluded.component.css']
})
export class FoliosExcludedComponent implements OnInit {

  folioExclude: IFolio = {} as IFolio
  fechaString = new Date().toLocaleString();
  isCalling = false
  page = 1


  constructor(private folioService: FolioService, private spinner: NgxSpinnerService) {


  }

  ngOnInit(): void {

    this.getFolios(1, 0)

  }

  refreshFolios() {
    this.getFolios(this.page, this.folioExclude.version)
  }

  getFolios(page: number, version: number) {
    this.isCalling = true
    this.spinner.show()
    this.folioService.getFoliosExcluded(page, version).subscribe(response => {
      this.validateNextCall(response)
      this.folioExclude = response
      this.isCalling = false
      this.spinner.hide()
    })
  }

  validateNextCall(folio: IFolio) {

    let nextCall = 0

    if (folio.next_status == 'final_eod') {
      return
    }
    else if (folio.next_status == 'pre_eod') {
      this.assignNextCall(folio.next_update, folio.version)
    }
    else if (this.folioExclude.version == folio.version) {
      nextCall = Math.floor(Math.random() * 60) * 1000
      this.assignNextCall(nextCall, folio.version)
    }
    else {
      if (folio.next_update > 0) {
        nextCall = folio.next_update * 1000
        this.assignNextCall(nextCall, folio.version)
      }
      else {
        nextCall = Math.floor(Math.random() * 60) * 1000
        this.assignNextCall(nextCall, folio.version)
      }
    }

  }

  assignNextCall(timeWait: number, version: number) {
    this.fechaString = new Date().toLocaleString();
    setTimeout(() => {
      this.getFolios(this.page, version)
    }, timeWait)
  }

  sortByFolio() {
    let { data } = this.folioExclude
    data.sort((a, b) => a.folio - b.folio)
  }

}
