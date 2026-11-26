import Extracted.JoinGuard
import Zeebe

open Aeneas Aeneas.Std Result ControlFlow
open join_guard

namespace Refinement

/-- `taken` as Zeebe's per-flow count (0 off the table). -/
def takenN (taken : Slice U32) (f : Nat) : Nat := (taken.val.getD f 0#u32).val

theorem takenN_of_ge {taken : Slice U32} {f : Nat} (h : taken.length ≤ f) :
    takenN taken f = 0 := by
  simp [takenN, List.getD_eq_getElem?_getD, List.getElem?_eq_none (by simpa using h)]

theorem takenN_of_lt {taken : Slice U32} {f : Nat} (h : f < taken.length) :
    takenN taken f = (taken.val[f]'(by simpa using h)).val := by
  simp [takenN, List.getD_eq_getElem?_getD, List.getElem?_eq_getElem (by simpa using h)]

/-- The kernel's open-edge test, over naturals. -/
def KOpen (taken : Slice U32) (j t f : Nat) : Prop := t ≠ j ∨ takenN taken f = 0

@[step]
theorem edge_open_spec (t f j : Usize) (taken : Slice U32) :
    edge_open t f j taken ⦃ b => (b = true ↔ KOpen taken j.val t.val f.val) ⦄ := by
  unfold edge_open
  by_cases h : t = j
  · subst h
    simp [core.slice.Slice.get, KOpen, takenN]
    cases hf : taken[f]? <;> simp_all
    exact ⟨fun e => by simp [e], fun e => UScalar.eq_of_val_eq (by simp [e])⟩
  · have : t.val ≠ j.val := fun e => h (UScalar.eq_of_val_eq e)
    simp [h, KOpen, this]

theorem distinct_taken_spec (incoming : Slice Usize) (taken : Slice U32) :
    distinct_taken incoming taken ⦃ c =>
      c.val = (incoming.val.filter (fun f => decide (0 < takenN taken f.val))).length ⦄ := by
  unfold distinct_taken distinct_taken_loop
  apply loop.spec_decr_nat (fun (p : Usize × Usize) => incoming.length - p.2.val)
    (fun p => p.2.val ≤ incoming.length ∧
      p.1.val = ((incoming.val.take p.2.val).filter (fun f => decide (0 < takenN taken f.val))).length)
  · rintro ⟨c, i⟩ ⟨hi, hc⟩
    simp only at hi hc ⊢
    unfold distinct_taken_loop.body
    dsimp only
    have hcle : c.val ≤ i.val := by
      rw [hc]; exact (List.length_filter_le _ _).trans (by simp)
    split
    · rename_i hlt
      have hlen : i.val < incoming.length := by scalar_tac
      have htake : incoming.val.take (i.val + 1) = incoming.val.take i.val ++ [incoming.val[i.val]] := by
        rw [List.take_add_one, List.getElem?_eq_getElem hlen]; rfl
      step*
      by_cases hf : flow < taken.len
      · simp only [hf, ite_true]
        have hfl : flow.val < taken.length := by scalar_tac
        step*
        by_cases hx : x > 0#u32
        · simp only [hx, ite_true]
          step*
          refine ⟨by scalar_tac, ?_, by scalar_tac⟩
          have hpos : 0 < takenN taken (incoming.val[i.val]).val := by
            rw [← flow_post, takenN_of_lt hfl, ← x_post]; scalar_tac
          rw [i3_post, htake, List.filter_append]
          simp [hpos, count1_post, hc]
        · simp only [hx, ite_false]
          step*
          refine ⟨by scalar_tac, ?_, by scalar_tac⟩
          have hz : takenN taken (incoming.val[i.val]).val = 0 := by
            rw [← flow_post, takenN_of_lt hfl, ← x_post]; scalar_tac
          rw [i3_post, htake, List.filter_append]
          simp [hz, hc]
      · simp only [hf, ite_false]
        step*
        refine ⟨by scalar_tac, ?_, by scalar_tac⟩
        have hz : takenN taken (incoming.val[i.val]).val = 0 := by
          rw [← flow_post]; exact takenN_of_ge (by scalar_tac)
        rw [i3_post, htake, List.filter_append]
        simp [hz, hc]
    · rename_i hge
      have : i.val = incoming.length := by scalar_tac
      simp [hc, this]
  · simp

/-! ## The breadth-first search -/

/-- A slice of `(target, flow)` edges, projected to naturals. -/
def edgesN (edges : Slice (Usize × Usize)) : List (Nat × Nat) :=
  edges.val.map (fun p => (p.1.val, p.2.val))

/-- The queue, projected to naturals. -/
def qN (q : alloc.vec.Vec Usize) : List Nat := q.val.map (·.val)

/-- The search's bookkeeping invariant: `visited` is the queue's
characteristic vector over `n` nodes, the queue holds no duplicates, and every
queued node satisfies `R` (it is reachable) and is not the join. -/
structure QInv (n : Nat) (R : Nat → Prop) (j : Nat) (vis : List Bool) (q : List Nat) : Prop where
  len : vis.length = n
  mem : ∀ v, v < n → (vis.getD v false = true ↔ v ∈ q)
  lt : ∀ x ∈ q, x < n
  nodup : q.Nodup
  reach : ∀ x ∈ q, R x ∧ x ≠ j

theorem QInv.length_le {n R j vis q} (h : QInv n R j vis q) : q.length ≤ n := by
  have := List.toFinset_card_of_nodup h.nodup
  rw [← this, ← Finset.card_range n]
  exact Finset.card_le_card (fun x hx => by simpa using h.lt x (by simpa using hx))

theorem QInv.length_lt {n R j vis q} (h : QInv n R j vis q) {t : Nat} (ht : t < n)
    (hnot : t ∉ q) : q.length < n := by
  have hsub : q.toFinset ⊆ (Finset.range n).erase t := fun x hx => by
    simp only [List.mem_toFinset] at hx
    simp only [Finset.mem_erase, Finset.mem_range]
    exact ⟨fun e => hnot (e ▸ hx), h.lt x hx⟩
  have := Finset.card_le_card hsub
  rw [List.toFinset_card_of_nodup h.nodup, Finset.card_erase_of_mem (by simpa using ht),
    Finset.card_range] at this
  omega

/-- Extending the processed-edge prefix by edge `i` once the queue has grown. -/
theorem done_extend {E : List (Nat × Nat)} {taken : Slice U32} {j n i : Nat} {Q Q' : List Nat}
    (hdone : ∀ k (_ : k < i) (hk : k < E.length), KOpen taken j (E[k]).1 (E[k]).2 →
      (E[k]).1 ≠ j ∧ ((E[k]).1 < n → (E[k]).1 ∈ Q))
    (hsub : Q <+: Q') (hi : i < E.length)
    (hnew : KOpen taken j (E[i]).1 (E[i]).2 → (E[i]).1 ≠ j ∧ ((E[i]).1 < n → (E[i]).1 ∈ Q')) :
    ∀ k (_ : k < i + 1) (hk : k < E.length), KOpen taken j (E[k]).1 (E[k]).2 →
      (E[k]).1 ≠ j ∧ ((E[k]).1 < n → (E[k]).1 ∈ Q') := by
  intro k hk hk' ho
  rcases Nat.lt_succ_iff_lt_or_eq.mp hk with hk | rfl
  · obtain ⟨h1, h2⟩ := hdone k hk hk' ho
    exact ⟨h1, fun h => hsub.subset (h2 h)⟩
  · exact hnew ho

theorem enqueue_spec (edges : Slice (Usize × Usize)) (join : Usize) (taken : Slice U32)
    (n : Nat) (R : Nat → Prop)
    (hR : ∀ t f, (t, f) ∈ edgesN edges → KOpen taken join.val t f → R t)
    (vis : Slice Bool) (q : alloc.vec.Vec Usize)
    (hinv : QInv n R join.val vis.val (qN q)) :
    enqueue_open_successors edges join taken vis q ⦃ r =>
      (r.1 = true → R join.val) ∧
      (r.1 = false → QInv n R join.val r.2.1.val (qN r.2.2) ∧ qN q <+: qN r.2.2 ∧
        ∀ t f, (t, f) ∈ edgesN edges → KOpen taken join.val t f →
          t ≠ join.val ∧ (t < n → t ∈ qN r.2.2)) ⦄ := by
  unfold enqueue_open_successors enqueue_open_successors_loop
  apply loop.spec_decr_nat (fun (st : Slice Bool × alloc.vec.Vec Usize × Usize) => edges.length - st.2.2.val)
    (fun st => QInv n R join.val st.1.val (qN st.2.1) ∧ qN q <+: qN st.2.1 ∧
      st.2.2.val ≤ edges.length ∧
      ∀ k (hk : k < st.2.2.val) (hk' : k < (edgesN edges).length),
        KOpen taken join.val ((edgesN edges)[k]).1 ((edgesN edges)[k]).2 →
          ((edgesN edges)[k]).1 ≠ join.val ∧
            (((edgesN edges)[k]).1 < n → ((edgesN edges)[k]).1 ∈ qN st.2.1))
  · rintro ⟨v, qq, i⟩ ⟨hq, hpre, hi, hdone⟩
    simp only at hq hpre hi hdone ⊢
    unfold enqueue_open_successors_loop.body
    dsimp only
    split
    · rename_i hlt
      step
      obtain ⟨t, f⟩ := p
      have hik : i.val < (edgesN edges).length := by simp [edgesN]; scalar_tac
      have hek : (edgesN edges)[i.val] = (t.val, f.val) := by simp [edgesN, ← p_post]
      have hedge : (t.val, f.val) ∈ edgesN edges := hek ▸ List.getElem_mem hik
      have hvlen : v.length = n := by simpa using hq.len
      step as ⟨b, b_post⟩
      split
      · rename_i hb
        have ho : KOpen taken join.val t.val f.val := b_post.mp hb
        split
        · rename_i htj
          subst htj
          simpa using hR _ _ hedge ho
        · rename_i htj
          have htj' : t.val ≠ join.val := fun e => htj (UScalar.eq_of_val_eq e)
          split
          · rename_i htn
            have htn' : t.val < n := by scalar_tac
            step as ⟨b1, b1_post⟩
            split
            · rename_i hvis
              step as ⟨i3, i3_post⟩
              refine ⟨hq, hpre, by scalar_tac, ?_, by scalar_tac⟩
              rw [i3_post]
              refine done_extend hdone (List.prefix_refl _) hik ?_
              intro _
              rw [hek]
              refine ⟨htj', fun _ => (hq.mem _ htn').mp ?_⟩
              simp_all
            · rename_i hvis
              have hnot : t.val ∉ qN qq := fun hm => hvis (by
                have := (hq.mem _ htn').mpr hm
                simp_all)
              have hqlen : (qN qq).length < n := hq.length_lt htn' hnot
              have hvmax : v.length ≤ Usize.max := by scalar_tac
              have hqmax : qq.val.length < Usize.max := by simp [qN] at hqlen; omega
              step as ⟨s, s_post⟩
              step as ⟨queue1, queue1_post⟩
              step as ⟨i3, i3_post⟩
              have hq1 : qN queue1 = qN qq ++ [t.val] := by simp [qN, queue1_post]
              have hpre1 : qN qq <+: qN queue1 := hq1 ▸ List.prefix_append _ _
              refine ⟨?_, hpre.trans hpre1, by scalar_tac, ?_, by scalar_tac⟩
              · rw [hq1, s_post]
                refine ⟨by simp [hvlen], ?_, ?_, ?_, ?_⟩
                · intro w hw
                  by_cases hwt : w = t.val
                  · subst hwt; simp [List.getD_eq_getElem?_getD, hvlen, htn']
                  · have := hq.mem w hw
                    simp only [List.getD_eq_getElem?_getD] at this ⊢
                    simp only [Slice.set, Slice.setAtNat, Slice.from_val, List.getElem?_set_ne (Ne.symm hwt),
                      List.mem_append, List.mem_singleton, hwt, or_false]
                    exact this
                · intro x hx
                  simp only [List.mem_append, List.mem_singleton] at hx
                  rcases hx with hx | rfl
                  · exact hq.lt x hx
                  · exact htn'
                · exact List.nodup_append.mpr ⟨hq.nodup, by simp, fun a ha b hb => by
                    simp only [List.mem_singleton] at hb
                    subst hb
                    exact fun e => hnot (e ▸ ha)⟩
                · intro x hx
                  simp only [List.mem_append, List.mem_singleton] at hx
                  rcases hx with hx | rfl
                  · exact hq.reach x hx
                  · exact ⟨hR _ _ hedge ho, htj'⟩
              · rw [i3_post]
                refine done_extend hdone hpre1 hik ?_
                intro _
                rw [hek]
                exact ⟨htj', fun _ => by simp [hq1]⟩
          · rename_i htn
            step as ⟨i2, i2_post⟩
            refine ⟨hq, hpre, by scalar_tac, ?_, by scalar_tac⟩
            rw [i2_post]
            refine done_extend hdone (List.prefix_refl _) hik ?_
            intro _
            rw [hek]
            exact ⟨htj', fun h => absurd h (by scalar_tac)⟩
      · rename_i hb
        step as ⟨i2, i2_post⟩
        refine ⟨hq, hpre, by scalar_tac, ?_, by scalar_tac⟩
        rw [i2_post]
        refine done_extend hdone (List.prefix_refl _) hik ?_
        intro ho
        rw [hek] at ho
        exact absurd (b_post.mpr ho) hb
    · rename_i hge
      have hie : i.val = edges.length := by scalar_tac
      simp only [WP.spec_ok]
      refine ⟨fun h => by simp at h, fun _ => ⟨hq, hpre, ?_⟩⟩
      intro t f hm ho
      obtain ⟨k, hk, hkv⟩ := List.mem_iff_getElem.mp hm
      have hl : (edgesN edges).length = edges.length := by simp [edgesN]
      have := hdone k (by scalar_tac) hk (by rw [hkv]; exact ho)
      rw [hkv] at this
      exact this
  · exact ⟨hinv, List.prefix_refl _, by simp, fun k hk => absurd hk (by simp)⟩

end Refinement
