import { catchError, retry, throwError } from 'rxjs';
import { Component, ElementRef, OnInit, Renderer2, ViewChild } from '@angular/core';
import { currencies, issuer_types, rates, ratings } from './utils/data';
import { FormBuilder, FormControl, FormGroup, Validators } from '@angular/forms';
import { HttpErrorResponse } from '@angular/common/http';
import { Isin, IsinFilterResult, TitlesRequest } from 'src/models/models';
import { NgxSpinnerService } from 'ngx-spinner';
import { SearchService } from './services/search.service';
import * as XLSX from 'xlsx';

declare var window: any;

@Component({
  selector: 'isin-search',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss'],
})
export class AppComponent implements OnInit {
  title = 'isin-search';
  searchIsinForm!: FormGroup;
  isinDisplay!: string;
  dateDisplay!: string;

  filterIsinesForm!: FormGroup;
  page = 1
  pageSize = 50
  ratings = ratings;
  rates = rates;
  currencies = currencies;
  class_names = issuer_types;

  seeTable = false;

  isin!: Isin;
  today = new Date();
  todayString = this.today.toISOString();
  issuers: any[] = [];
  currentlyVisibleIsines: IsinFilterResult[] = [];
  isines: IsinFilterResult[] = [];

  titleRequest!: TitlesRequest;

  @ViewChild('filteredIsines') table!: ElementRef;

  issuer: string = '';

  spinnerIsin = 'spinnerIsinSearch';
  spinnerIsinesSearch = 'spinnerIsinesSearch';
  spinnerParametrize = 'spinnerParametrize';

  ascending: boolean[] = new Array(9).fill(true);

  showSpinner(name: string) {
    this.spinner.show(name);
  }

  constructor(
    private fb: FormBuilder,
    private ss: SearchService,
    private spinner: NgxSpinnerService,

  ) {
    this.searchIsinForm = this.fb.group({
      isin: new FormControl(null, Validators.required),
      valuationDate: new FormControl(null, Validators.required),
    });

    this.filterIsinesForm = this.fb.group({
      valuationDate: new FormControl(null, Validators.required),
      issuer: new FormControl(null, Validators.required),
      minMatDays: new FormControl(null, [
        Validators.required,
        Validators.min(1),
        Validators.max(99999),
        Validators.pattern('^[0-9]*$'),
      ]),
      maxMatDays: new FormControl(null, [
        Validators.required,
        Validators.min(1),
        Validators.max(99999),
        Validators.pattern('^[0-9]*$'),
      ]),
      rateType: new FormControl(null, Validators.required),
      rating: new FormControl(null, Validators.required),
      minYield: new FormControl(null, Validators.required),
      maxYield: new FormControl(null, Validators.required),
      currency: new FormControl(null, Validators.required),
      class_name: new FormControl(null, Validators.required),
    });

    this.titleRequest = {
      issuer: null,
      rating: null,
      rate_type: null,
      currency: null,
      maturity_days: {
        min: null,
        max: null,
      },
      yield: {
        min: null,
        max: null,
      },
      class_name: null,
    };
  }

  emptySearch() {
    console.log(this.filterIsinesForm.get('minMatDays')?.value);
    //this.renderer.setProperty(this.typeaheadInput.nativeElement, 'innerHTML', '');
    //this.renderer.setValue(this.typeaheadInput.nativeElement, '');

    const emptyIssuerEvent = new CustomEvent('empty-typeahead', {
      detail: {
        empty: true,
      },
    });
    window.dispatchEvent(emptyIssuerEvent);

    this.filterIsinesForm.reset();
  }

  ngOnInit(): void {
    //this.searchIsinForm.controls['valuationDate'].setValue(this.todayString);

    this.ss.getIssuers().subscribe((_issuers: any) => {
      this.issuers = _issuers.map((issuer: any) => issuer.issuer);
      this.issuers.unshift('NA');
      this.issuers.sort((a: string, b: string) => {
        if (a < b) {
          return -1;
        }
        if (a > b) {
          return 1;
        }
        return 0;
      });
    });
  }

  sortText(ascending: boolean) {
    ascending = !ascending;
  }

  sortTable(column: string) {
    switch (column) {
      case 'isin':
        if (this.ascending[0]) {
          this.ascending[0] = !this.ascending[0];
          this.currentlyVisibleIsines.sort((a: IsinFilterResult, b: IsinFilterResult) => {
            let fa = a.isin.toLowerCase();
            let fb = b.isin.toLowerCase();
            if (fa < fb) {
              return -1;
            }
            if (fa > fb) {
              return 1;
            }
            return 0;
          });
        } else {
          this.ascending[0] = !this.ascending[0];
          this.currentlyVisibleIsines.sort((a: IsinFilterResult, b: IsinFilterResult) => {
            let fa = a.isin.toLowerCase();
            let fb = b.isin.toLowerCase();
            if (fa < fb) {
              return 1;
            }
            if (fa > fb) {
              return -1;
            }
            return 0;
          });
        }
        break;

      case 'nemo':
        if (this.ascending[0]) {
          this.ascending[0] = !this.ascending[0];
          this.currentlyVisibleIsines.sort((a: IsinFilterResult, b: IsinFilterResult) => {
            let fa = a.nemo.toLowerCase();
            let fb = b.nemo.toLowerCase();
            if (fa < fb) {
              return -1;
            }
            if (fa > fb) {
              return 1;
            }
            return 0;
          });
        } else {
          this.ascending[0] = !this.ascending[0];
          this.currentlyVisibleIsines.sort((a: IsinFilterResult, b: IsinFilterResult) => {
            let fa = a.nemo.toLowerCase();
            let fb = b.nemo.toLowerCase();
            if (fa < fb) {
              return 1;
            }
            if (fa > fb) {
              return -1;
            }
            return 0;
          });
        }
        break;

      case 'issuer_name':
        if (this.ascending[0]) {
          this.ascending[0] = !this.ascending[0];
          this.currentlyVisibleIsines.sort((a: IsinFilterResult, b: IsinFilterResult) => {
            let fa = a.issuer_name?.toLowerCase() ?? '';
            let fb = b.issuer_name?.toLowerCase() ?? '';
            if (fa < fb) {
              return -1;
            }
            if (fa > fb) {
              return 1;
            }
            return 0;
          });
        } else {
          this.ascending[0] = !this.ascending[0];
          this.currentlyVisibleIsines.sort((a: IsinFilterResult, b: IsinFilterResult) => {
            let fa = a.issuer_name?.toLowerCase() ?? '';
            let fb = b.issuer_name?.toLowerCase() ?? '';
            if (fa < fb) {
              return 1;
            }
            if (fa > fb) {
              return -1;
            }
            return 0;
          });
        }
        break;

      case 'yield':
        if (this.ascending[0]) {
          this.ascending[0] = !this.ascending[0];
          this.currentlyVisibleIsines.sort((a: IsinFilterResult, b: IsinFilterResult) => {
            return a.yield - b.yield;
          });
        } else {
          this.ascending[0] = !this.ascending[0];
          this.currentlyVisibleIsines.sort((a: IsinFilterResult, b: IsinFilterResult) => {
            return b.yield - a.yield;
          });
        }
        break;
      case 'maturity_days':
        if (this.ascending[0]) {
          this.ascending[0] = !this.ascending[0];
          this.currentlyVisibleIsines.sort((a: IsinFilterResult, b: IsinFilterResult) => {
            return a.maturity_days - b.maturity_days;
          });
        } else {
          this.ascending[0] = !this.ascending[0];
          this.currentlyVisibleIsines.sort((a: IsinFilterResult, b: IsinFilterResult) => {
            return b.maturity_days - a.maturity_days;
          });
        }
        break;

      case 'real_rating':
        if (this.ascending[0]) {
          this.ascending[0] = !this.ascending[0];
          this.currentlyVisibleIsines.sort((a: IsinFilterResult, b: IsinFilterResult) => {
            let fa = a.real_rating?.toLowerCase() ?? '';
            let fb = b.real_rating?.toLowerCase() ?? '';
            if (fa < fb) {
              return -1;
            }
            if (fa > fb) {
              return 1;
            }
            return 0;
          });
        } else {
          this.ascending[0] = !this.ascending[0];
          this.currentlyVisibleIsines.sort((a: IsinFilterResult, b: IsinFilterResult) => {
            let fa = a.real_rating?.toLowerCase() ?? '';
            let fb = b.real_rating?.toLowerCase() ?? '';
            if (fa < fb) {
              return 1;
            }
            if (fa > fb) {
              return -1;
            }
            return 0;
          });
        }
        break;
      case 'rate_type':
        if (this.ascending[0]) {
          this.ascending[0] = !this.ascending[0];
          this.currentlyVisibleIsines.sort((a: IsinFilterResult, b: IsinFilterResult) => {
            let fa = a.rate_type?.toLowerCase() ?? '';
            let fb = b.rate_type?.toLowerCase() ?? '';
            if (fa < fb) {
              return -1;
            }
            if (fa > fb) {
              return 1;
            }
            return 0;
          });
        } else {
          this.ascending[0] = !this.ascending[0];
          this.currentlyVisibleIsines.sort((a: IsinFilterResult, b: IsinFilterResult) => {
            let fa = a.rate_type?.toLowerCase() ?? '';
            let fb = b.rate_type?.toLowerCase() ?? '';
            if (fa < fb) {
              return 1;
            }
            if (fa > fb) {
              return -1;
            }
            return 0;
          });
        }
        break;
      case 'equivalent_margin':
        if (this.ascending[0]) {
          this.ascending[0] = !this.ascending[0];
          this.currentlyVisibleIsines.sort((a: IsinFilterResult, b: IsinFilterResult) => {
            if (!b.equivalent_margin || !a.equivalent_margin) {
              b.equivalent_margin = 0;
              a.equivalent_margin = 0;
            }
            return a.equivalent_margin ?? 0 - b.equivalent_margin ?? 0;
          });
        } else {
          this.ascending[0] = !this.ascending[0];
          this.currentlyVisibleIsines.sort((a: IsinFilterResult, b: IsinFilterResult) => {
            if (!b.equivalent_margin || !a.equivalent_margin) {
              b.equivalent_margin = 0;
              a.equivalent_margin = 0;
            }
            return b.equivalent_margin ?? 0 - a.equivalent_margin ?? 0;
          });
        }
        break;
      case 'currency_type':
        if (this.ascending[0]) {
          this.ascending[0] = !this.ascending[0];
          this.currentlyVisibleIsines.sort((a: IsinFilterResult, b: IsinFilterResult) => {
            let fa = a.currency_type?.toLowerCase() ?? '';
            let fb = b.currency_type?.toLowerCase() ?? '';
            if (fa < fb) {
              return -1;
            }
            if (fa > fb) {
              return 1;
            }
            return 0;
          });
        } else {
          this.ascending[0] = !this.ascending[0];
          this.currentlyVisibleIsines.sort((a: IsinFilterResult, b: IsinFilterResult) => {
            let fa = a.currency_type?.toLowerCase() ?? '';
            let fb = b.currency_type?.toLowerCase() ?? '';
            if (fa < fb) {
              return 1;
            }
            if (fa > fb) {
              return -1;
            }
            return 0;
          });
        }
        break;
      default:
        break;
    }
  }

  private handleError(error: HttpErrorResponse) {
    alert('No se encontró el ISIN');

    if (error.status === 0) {
      // A client-side or network error occurred. Handle it accordingly.
      console.error('An error occurred:', error.error);
    } else {
      // The backend returned an unsuccessful response code.
      // The response body may contain clues as to what went wrong.
      console.error(
        `Backend returned code ${error.status}, body was: `,
        error.error
      );
    }
    // Return an observable with a user-facing error message.
    return throwError(
      () => new Error('Something bad happened; please try again later.')
    );
  }

  private saveIsinesError(error: HttpErrorResponse) {
    alert('Hubo un error al parametrizar los isines');

    if (error.status === 0) {
      // A client-side or network error occurred. Handle it accordingly.
      console.error('An error occurred:', error.error);
    } else {
      // The backend returned an unsuccessful response code.
      // The response body may contain clues as to what went wrong.
      console.error(
        `Backend returned code ${error.status}, body was: `,
        error.error
      );
    }
    // Return an observable with a user-facing error message.
    return throwError(
      () => new Error('Something bad happened; please try again later.')
    );
  }

  search() {
    this.showSpinner(this.spinnerIsin);
    let isinToSearch = this.searchIsinForm.get('isin')?.value;
    this.ss
      .getIsin(isinToSearch)
      .pipe(retry(0), catchError(this.handleError))
      .subscribe(
        (response: any) => {
          this.spinner.hide(this.spinnerIsin);
          this.isin = response.data;
          this.isinDisplay = this.searchIsinForm.get('isin')?.value;

          //this.dateDisplay = this.searchIsinForm.get('valuationDate')?.value.toString().replaceAll("-","/");
        },
        (error) => {
          this.spinner.hide(this.spinnerIsin);
        }
      );
  }

  validateNull(field: any) {
    if (field == '') {
      return null;
    }
    return field;
  }

  setIssuer(input: string) {
    this.filterIsinesForm.get('issuer')?.setValue(input);
    this.issuer = input;
  }

  searchTitles() {

    console.log(this.filterIsinesForm.value)

    this.titleRequest.issuer = this.validateNull(this.issuer);
    this.titleRequest.rating = this.validateNull(
      this.filterIsinesForm.get('rating')?.value
    );
    this.titleRequest.rate_type = this.validateNull(
      this.filterIsinesForm.get('rateType')?.value
    );
    this.titleRequest.yield.min = this.filterIsinesForm.get('minYield')?.value;
    this.titleRequest.yield.max = this.filterIsinesForm.get('maxYield')?.value;
    this.titleRequest.maturity_days.min =
      this.filterIsinesForm.get('minMatDays')?.value;
    this.titleRequest.maturity_days.max =
      this.filterIsinesForm.get('maxMatDays')?.value;
    this.titleRequest.currency = this.validateNull(
      this.filterIsinesForm.get('currency')?.value
    );
    this.titleRequest.class_name = this.validateNull(
      this.filterIsinesForm.get('class_name')?.value
    );

    this.showSpinner(this.spinnerIsinesSearch);

    this.ss.getTitles(this.titleRequest).subscribe(
      (response: any) => {
        this.isines = response.data;
        if (this.isines.length > 0) {
          this.seeTable = true;
          this.refreshFolios()
          alert(
            `Se han encontrado ${this.isines.length} ${this.isines.length != 1 ? 'ISINES' : 'ISIN'
            } que cumplen los criterios especificados. Desliza hacia abajo para verlos.`
          );
          this.spinner.hide(this.spinnerIsinesSearch);
        } else {
          this.seeTable = false;
          alert(
            'No se encontraron ISINES que cumplan con los criterios de búsqueda.'
          );
        }
      },
    )

  }

  get isFormValid() {
    return (
      this.issuer != undefined ||
      this.filterIsinesForm.get('rating')?.valid ||
      this.filterIsinesForm.get('rateType')?.valid ||
      this.filterIsinesForm.get('minYield')?.valid ||
      this.filterIsinesForm.get('maxYield')?.valid ||
      this.filterIsinesForm.get('minMatDays')?.valid ||
      this.filterIsinesForm.get('maxMatDays')?.valid ||
      this.filterIsinesForm.get('currency')?.valid
    );
  }

  test() {
    const minMatDaysControl = this.filterIsinesForm.get('minMatDays');
    if (minMatDaysControl?.errors?.['min']) {
      return minMatDaysControl.errors['min'];
    }
  }

  saveIsines() {
    let isines_ = this.isines.map((isin) => isin.isin);
    this.spinner.show(this.spinnerParametrize);
    this.ss
      .saveIsines(isines_)
      .pipe(retry(0), catchError(this.saveIsinesError))
      .subscribe((response) => {
        alert(
          'Se han parametrizado los isines. Los podrá ver en el Seguimiento de Isines.  '
        );
        this.spinner.hide(this.spinnerParametrize);
        const reloadEvent = new CustomEvent('reload-isin-track', {
          detail: {
            reload: true,
          },
        });
        window.dispatchEvent(reloadEvent);
      });
  }

  downloadIsinesTable() {
    const ws: XLSX.WorkSheet = XLSX.utils.table_to_sheet(
      this.table.nativeElement
    );

    let date = new Date();
    /* new format */
    var fmt = '0.00';
    /* change cell format of range B2:D4 */
    var range = { s: { r: 1, c: 1 }, e: { r: 2, c: 100000 } };
    for (var R = range.s.r; R <= range.e.r; ++R) {
      for (var C = range.s.c; C <= range.e.c; ++C) {
        var cell = ws[XLSX.utils.encode_cell({ r: R, c: C })];
        if (!cell || cell.t != 'n') continue; // only format numeric cells
        cell.z = fmt;
      }
    }
    const wb: XLSX.WorkBook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, 'Hoja 1');
    var fmt = '@';
    wb.Sheets['Hoja 1']['F'] = fmt;

    /* save to file */
    XLSX.writeFile(
      wb,
      `isines-filtrados-${date.toISOString().split('T')[0]}_${date
        .getHours()
        .toString()}${date.getMinutes().toString()}${date
          .getSeconds()
          .toString()}.xlsx`
    );
  }

  refreshFolios() {
    this.currentlyVisibleIsines = this.isines.map((folio, i) => ({ id: i + 1, ...folio })).slice(
      (this.page - 1) * this.pageSize,
      (this.page - 1) * this.pageSize + this.pageSize,
    )
  }

  sortByFolio() {
    this.currentlyVisibleIsines.sort((a, b) => (a.isin > b.isin) ? 1 : ((b.isin > a.isin) ? -1 : 0))
  }

}
