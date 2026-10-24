/-!
Functional model of the Go `store.Store` map operations (`store/store.go`).

We model the map as a lookup function `α → Option β`, which matches Go's `map[K]V`
semantics (at most one value per key). Mutexes are omitted.
-/

namespace KvStore

variable {α β : Type} [DecidableEq α]

/-- Abstract map: `none` means the key is absent (Go's `ok = false`). -/
abbrev Data (α β : Type) := α → Option β

/-! ### Core operations -/

def getData (data : Data α β) (k : α) : Option β :=
  data k

def setData (data : Data α β) (k : α) (v : β) : Data α β :=
  fun k' => if _h : k' = k then some v else data k'

def deleteData (data : Data α β) (k : α) : Data α β :=
  fun k' => if _h : k' = k then none else data k'

def emptyData : Data α β :=
  fun _ => none

/-- Build a map from a list; later entries override earlier ones for the same key. -/
def ofList : List (α × β) → Data α β
  | [] => emptyData
  | (k, v) :: rest => setData (ofList rest) k v

structure Store (α β : Type) [DecidableEq α] where
  data : Data α β
  deriving Inhabited

def New (α β : Type) [DecidableEq α] : Store α β :=
  ⟨emptyData⟩

def Get (s : Store α β) (k : α) : Option β :=
  getData s.data k

def Set (s : Store α β) (k : α) (v : β) : Store α β :=
  ⟨setData s.data k v⟩

def Delete (s : Store α β) (k : α) : Store α β :=
  ⟨deleteData s.data k⟩

/-- `Replace` in Go swaps the whole `map`; we rebuild from the given pairs. -/
def Replace (_s : Store α β) (pairs : List (α × β)) : Store α β :=
  ⟨ofList pairs⟩

/-- Approximate `Len` over a finite key universe you care about (tests, snapshots). -/
def Len (s : Store α β) (keys : List α) : Nat :=
  keys.countP fun k => (Get s k).isSome

end KvStore
