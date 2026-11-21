// The bug src/boxes.ts rules out, kept as a regression: its law must be refuted (test/check_laws.ts).

// The live count grows past the cap.
export class Uncapped {
  constructor(readonly cap: number) {}

  live = 0

  // gdp: law a new box never takes the live count past the cap: live <= old(live) || old(live) < cap
  open(): boolean {
    this.live = this.live + 1
    return true
  }
}
