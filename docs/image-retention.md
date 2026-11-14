# Image retention

Automatic provider snapshot deletion is **disabled**. `ensure_image()` may
build missing layers, but its compatibility `prune_snapshots()` entry point only
records local image ordering. It never lists candidates or calls `snapshot rm`.
A shared prefix, image age, or last-two cache is not evidence of exclusive use.

## Pin a packet or GO before publishing it

On an existing authenticated controller with `image_pins_protocol: 1`:

```sh
supercomputer image pin sc-f0d8daeee6c3 --kind packet --reference calibration-v6
supercomputer image pin sc-f0d8daeee6c3 --kind go --reference calibration-go-6
supercomputer image pins
```

The register operation is also available as `boat.image_pins('register',
images=[...], kind='packet', reference='...')`, or authenticated POST
`/image-pins/register` with `images`, `kind`, and `reference`. Listing is POST
`/image-pins/list`. No operation starts a local controller. Require a successful
registration receipt **before** publishing the packet or GO. The receipt contains
registry epoch/revision, controller-observed `accepted_at`, and `protected_until`.
Client timestamps and requested shorter TTLs are not accepted as authority.

Use a stable reference and an explicit, ordered list of exact snapshot names.
Include any dependency snapshots that the packet needs to recreate its image.
Packet, GO and internal batch references occupy separate namespaces. Repeating
an identical binding preserves or extends protection; changing its images is
refused. Registration retains images for **at least seven days**, including the
boundary. There is no public unpin or force-expire operation.

## Live ownership and persistence

Every accepted batch receives a durable image pin before its queue file is
published or admission acknowledged. Recovery backfills old queue admissions.
Live pins have no TTL. They are closed only following a positive durable cleanup
receipt, followed by at least seven days' grace. Disappearance of a batch, an
asynchronous legacy cancellation, or uncertain cleanup is not a release; such
pins may conservatively remain indefinitely. Duplicate admissions cannot change
their image. Listing unions these pins with current batches, all locally known
resident workers (including stopped/uncertain workers), and pending creations.

`live/scheduler/image-pins.json` uses strict versioned records, atomic replacement,
file and directory fsync, and local interprocess locking. Corrupt, regressed,
missing previously-observed state, or uncertain writes fail closed. The file is
captured with admissions under the state-export guard. Handoffs preserve exact
pin history and refuse retention-unaware targets when records exist.

## Operational boundary: GC remains off

The listing reports `gc_enabled: false` and `inventory_complete: false` on purpose.
Local worker records are not a complete provider inventory. Reading pins and
then deleting outside a lock would still race new admissions, builds or creates.
This release does **not** implement a deletion lease or authorize automatic GC.
Consequently even an unregistered historic packet, an expired pin, or an image
unknown to this controller cannot trigger client automatic deletion.

Old installed clients with provider credentials can still run their old pruner.
Update or retire **every** such client in the operator-authorized rollout gap;
updating only the controller does not fence their provider credentials. No
controller/worker rollout is implied by merging this source change. Explicit
operator/provider `snapshot rm` is outside this automatic-retention contract.

Before any future GC is enabled, establish one fenced deletion authority,
coordinate all builders and in-flight creations, backfill the authoritative
last-seven-days packet/GO list, and require complete fresh resident inventory.
Persist a deletion reservation that excludes new pins before provider I/O and
retain it through uncertain responses. Current asynchronous controller backups
can lose recent acknowledged pins after total controller loss; restore must
keep GC disabled until receipts and historic events are reconciled. This release
makes no stronger cross-host or arbitrary-controller-loss durability claim.
