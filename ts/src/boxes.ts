// The boxes one client holds live (creating, running a job, or between jobs), against its cap.
export class Boxes {
  constructor(readonly cap: number) {}

  live = 0

  // gdp: law a new box never takes the live count past the cap: live <= old(live) || old(live) < cap
  // gdp: law a box is opened only below the cap: !result || old(live) < cap
  open(): boolean {
    if (this.live < this.cap) {
      this.live = this.live + 1
      return true
    }
    return false
  }

  // gdp: law closing never makes boxes appear: live <= old(live)
  close(): void {
    if (this.live > 0) this.live = this.live - 1
  }
}
