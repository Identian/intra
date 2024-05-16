import { ComponentFixture, TestBed } from '@angular/core/testing';

import { NgbdTypeaheadBasicComponent } from './ngbd-typeahead-basic.component';

describe('NgbdTypeaheadBasicComponent', () => {
  let component: NgbdTypeaheadBasicComponent;
  let fixture: ComponentFixture<NgbdTypeaheadBasicComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ NgbdTypeaheadBasicComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(NgbdTypeaheadBasicComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
