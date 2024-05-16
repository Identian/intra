import { ComponentFixture, TestBed } from '@angular/core/testing';

import { UpdateIndicatorComponent } from './update-indicator.component';

describe('UpdateIndicatorComponent', () => {
  let component: UpdateIndicatorComponent;
  let fixture: ComponentFixture<UpdateIndicatorComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ UpdateIndicatorComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(UpdateIndicatorComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
