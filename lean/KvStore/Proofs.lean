import KvStore.Map

namespace KvStore

variable {α β : Type} [DecidableEq α]

/-! ## `set` laws -/

theorem get_set_self (data : Data α β) (k : α) (v : β) :
    getData (setData data k v) k = some v := by
  simp [getData, setData]

theorem get_set_other {data : Data α β} {k k' : α} {v : β} (h : k' ≠ k) :
    getData (setData data k v) k' = getData data k' := by
  simp [getData, setData, h]

/-! ## `delete` laws -/

theorem get_delete_self (data : Data α β) (k : α) :
    getData (deleteData data k) k = none := by
  simp [getData, deleteData]

theorem get_delete_other {data : Data α β} {k k' : α} (h : k' ≠ k) :
    getData (deleteData data k) k' = getData data k' := by
  simp [getData, deleteData, h]

/-! ## Empty store -/

theorem get_empty (k : α) : getData (emptyData : Data α β) k = none := rfl

/-! ## `Store` wrapper theorems (mirror Go names) -/

theorem Get_Set_self (s : Store α β) (k : α) (v : β) :
    Get (Set s k v) k = some v :=
  get_set_self s.data k v

theorem Get_Set_other {s : Store α β} {k k' : α} {v : β} (h : k' ≠ k) :
    Get (Set s k v) k' = Get s k' :=
  get_set_other (data := s.data) h

theorem Get_Delete_self (s : Store α β) (k : α) :
    Get (Delete s k) k = none :=
  get_delete_self s.data k

theorem Get_Delete_other {s : Store α β} {k k' : α} (h : k' ≠ k) :
    Get (Delete s k) k' = Get s k' :=
  get_delete_other (data := s.data) h

theorem Get_New (k : α) : Get (New α β) k = none := by
  simp [Get, New, get_empty]

/-! ## `ofList` / `Replace` -/

theorem get_ofList_nil (k : α) : getData (ofList ([] : List (α × β))) k = none := by
  simp [ofList, get_empty]

theorem get_ofList_cons (k₀ : α) (v₀ : β) (rest : List (α × β)) (k : α) :
    getData (ofList ((k₀, v₀) :: rest)) k =
      if h : k = k₀ then some v₀ else getData (ofList rest) k := by
  simp [ofList, getData, setData, eq_comm]

theorem Replace_Get_ofList (s : Store α β) (pairs : List (α × β)) (k : α) :
    Get (Replace s pairs) k = getData (ofList pairs) k := rfl

/-! ## Composition: set then delete restores absent key -/

theorem get_set_delete {data : Data α β} {k : α} {v : β} :
    getData (deleteData (setData data k v) k) k = none := by
  simpa using get_delete_self (setData data k v) k

/-! ## Concrete example (Nat keys) -/

example : Get (Set (New Nat Nat) 1 42) 1 = some 42 :=
  Get_Set_self (New Nat Nat) 1 42

example : Get (Delete (Set (New Nat Nat) 1 42) 1) 1 = none := by
  simpa using Get_Delete_self (Set (New Nat Nat) 1 42) 1

end KvStore
