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

@[step]
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

/-- The kernel's graph, projected to naturals. -/
def graph (succ : Slice (alloc.vec.Vec (Usize × Usize))) : Zeebe.Graph :=
  succ.val.map (fun v => v.val.map (fun p => (p.1.val, p.2.val)))

/-- One step of the kernel's search: an open edge. -/
def KStep (succ : Slice (alloc.vec.Vec (Usize × Usize))) (taken : Slice U32) (j u v : Nat) : Prop :=
  ∃ f, (v, f) ∈ Zeebe.out (graph succ) u ∧ KOpen taken j v f

/-- Reachability over open edges. -/
def KReach (succ : Slice (alloc.vec.Vec (Usize × Usize))) (taken : Slice U32) (j s t : Nat) : Prop :=
  Relation.ReflTransGen (KStep succ taken j) s t

theorem out_graph {succ : Slice (alloc.vec.Vec (Usize × Usize))} {u : Nat}
    (h : u < succ.length) :
    Zeebe.out (graph succ) u = edgesN (alloc.vec.Vec.deref (succ.val[u]'(by simpa using h))) := by
  simp [Zeebe.out, graph, edgesN, alloc.vec.Vec.deref, Slice.from_val,
    List.getD_eq_getElem?_getD, List.getElem?_eq_getElem (by simpa using h)]

theorem out_graph_ge {succ : Slice (alloc.vec.Vec (Usize × Usize))} {u : Nat}
    (h : succ.length ≤ u) : Zeebe.out (graph succ) u = [] := by
  simp [Zeebe.out, graph, List.getD_eq_getElem?_getD, List.getElem?_eq_none (by simpa using h)]

/-- A queue closed under open edges, holding the source and not the join,
rules out a path to the join. -/
theorem not_reach_of_closed {succ : Slice (alloc.vec.Vec (Usize × Usize))} {taken : Slice U32}
    {j s : Nat} (Q : List Nat) (hs : s ∈ Q) (hQ : ∀ x ∈ Q, x ≠ j)
    (hclosed : ∀ x ∈ Q, ∀ t f, (t, f) ∈ Zeebe.out (graph succ) x → KOpen taken j t f →
      t ≠ j ∧ (t < succ.length → t ∈ Q)) :
    ¬ KReach succ taken j s j := by
  have key : ∀ v, KReach succ taken j s v → v ≠ j ∧ (v ∈ Q ∨ succ.length ≤ v) := by
    intro v hv
    induction hv with
    | refl => exact ⟨hQ s hs, Or.inl hs⟩
    | @tail u w _ hstep ih =>
      obtain ⟨f, hm, ho⟩ := hstep
      rcases ih.2 with hu | hu
      · obtain ⟨h1, h2⟩ := hclosed _ hu _ _ hm ho
        refine ⟨h1, ?_⟩
        by_cases hlt : w < succ.length
        · exact Or.inl (h2 hlt)
        · exact Or.inr (by omega)
      · rw [out_graph_ge hu] at hm
        simp at hm
  exact fun h => (key j h).1 rfl

/-- The search's loop invariant: the bookkeeping invariant over reachable
nodes, the source queued, and every edge out of an already-dequeued node
(`k < head`) either closed or leading to a queued node that is not the join. -/
def BfsInv (succ : Slice (alloc.vec.Vec (Usize × Usize))) (taken : Slice U32) (j s : Nat)
    (st : alloc.vec.Vec Bool × alloc.vec.Vec Usize × Usize) : Prop :=
  QInv succ.length (KReach succ taken j s) j st.1.val (qN st.2.1) ∧
  st.2.2.val ≤ (qN st.2.1).length ∧ s ∈ qN st.2.1 ∧
  ∀ k (_ : k < st.2.2.val) (hk : k < (qN st.2.1).length), ∀ t f,
    (t, f) ∈ Zeebe.out (graph succ) ((qN st.2.1)[k]) → KOpen taken j t f →
      t ≠ j ∧ (t < succ.length → t ∈ qN st.2.1)

theorem path_reaches_join_loop_spec (succ : Slice (alloc.vec.Vec (Usize × Usize)))
    (source join : Usize) (taken : Slice U32)
    (st : alloc.vec.Vec Bool × alloc.vec.Vec Usize × Usize)
    (hinv : BfsInv succ taken join.val source.val st) :
    path_reaches_join_loop succ join taken st.1 st.2.1 st.2.2 ⦃ b =>
      (b = true ↔ KReach succ taken join.val source.val join.val) ⦄ := by
  unfold path_reaches_join_loop
  apply loop.spec_decr_nat (fun (st : alloc.vec.Vec Bool × alloc.vec.Vec Usize × Usize) =>
      succ.length - st.2.2.val)
    (BfsInv succ taken join.val source.val)
  · rintro ⟨vis, q, head⟩ ⟨hq, hhead, hsrc, hclosed⟩
    simp only at hq hhead hsrc hclosed ⊢
    unfold path_reaches_join_loop.body
    dsimp only
    split
    · rename_i hlt
      step as ⟨node, node_post⟩
      have hql : head.val < (qN q).length := by simp [qN]; scalar_tac
      have hnode : node.val = (qN q)[head.val] := by simp [qN, node_post]
      have hnin : node.val ∈ qN q := hnode ▸ List.getElem_mem hql
      have hnlt : node.val < succ.length := hq.lt _ hnin
      step as ⟨head1, head1_post⟩
      step as ⟨v, v_post⟩
      have hout : Zeebe.out (graph succ) node.val = edgesN v.deref := by
        rw [out_graph hnlt, v_post]
      have hR : ∀ t f, (t, f) ∈ edgesN v.deref → KOpen taken join.val t f →
          KReach succ taken join.val source.val t :=
        fun t f hm ho => (hq.reach _ hnin).1.tail ⟨f, hout ▸ hm, ho⟩
      simp only [lift, alloc.vec.Vec.deref_mut, bind_ok]
      have hvs : vis.slice.val = vis.val := rfl
      apply WP.spec_bind (enqueue_spec v.deref join taken succ.length _ hR vis.slice q (hvs ▸ hq))
      rintro ⟨b, s2, queue1⟩ ⟨hb1, hb2⟩
      simp only at hb1 hb2
      rcases b with _ | _
      · apply WP.spec_ok _ |>.mpr
        obtain ⟨hq1, hpre1, hcl1⟩ := hb2 rfl
        have hlen1 := hpre1.length_le
        have hle := hq.length_le
        dsimp only
        refine ⟨⟨hq1, by simp [head1_post]; omega, hpre1.subset hsrc, ?_⟩, by omega⟩
        intro k hk hk1 t f hm ho
        dsimp only at hk hk1 hm ⊢
        have hk0 : k < (qN q).length := by omega
        have hget : (qN queue1)[k] = (qN q)[k] := (List.IsPrefix.getElem hpre1 hk0).symm
        rw [hget] at hm
        rcases Nat.lt_succ_iff_lt_or_eq.mp (head1_post ▸ hk) with hk' | rfl
        · obtain ⟨h1, h2⟩ := hclosed k hk' hk0 t f hm ho
          exact ⟨h1, fun h => hpre1.subset (h2 h)⟩
        · rw [← hnode, hout] at hm
          exact hcl1 t f hm ho
      · apply WP.spec_ok _ |>.mpr
        exact ⟨fun _ => hb1 rfl, fun _ => rfl⟩
    · rename_i hge
      simp only [WP.spec_ok]
      have hhl : head.val = (qN q).length := by simp [qN] at hhead ⊢; scalar_tac
      refine ⟨fun h => by simp at h, fun h => absurd h ?_⟩
      exact not_reach_of_closed (qN q) hsrc (fun x hx => (hq.reach x hx).2) (fun x hx t f hm ho => by
        obtain ⟨k, hk, rfl⟩ := List.mem_iff_getElem.mp hx
        exact hclosed k (by omega) hk t f hm ho)
  · exact hinv

@[step]
theorem path_reaches_join_spec (succ : Slice (alloc.vec.Vec (Usize × Usize)))
    (source join : Usize) (taken : Slice U32) :
    path_reaches_join succ source join taken ⦃ b =>
      (b = true ↔ KReach succ taken join.val source.val join.val) ⦄ := by
  unfold path_reaches_join
  split
  · rename_i h
    subst h
    exact (WP.spec_ok _).mpr ⟨fun _ => Relation.ReflTransGen.refl, fun _ => rfl⟩
  · rename_i hne
    have hne' : source.val ≠ join.val := fun e => hne (UScalar.eq_of_val_eq e)
    dsimp only
    split
    · rename_i hge
      apply (WP.spec_ok _).mpr
      simp only [Bool.false_eq_true, false_iff]
      intro h
      rcases Relation.ReflTransGen.cases_head h with e | ⟨c, ⟨f, hm, _⟩, _⟩
      · exact hne' e
      · rw [out_graph_ge (by scalar_tac)] at hm
        simp at hm
    · rename_i hlt
      step*
      have hn : source.val < succ.length := by scalar_tac
      have hq : qN ({ slice := y } : alloc.vec.Vec Usize) = [source.val] := by
        simp [qN, y_post, alloc.vec.Vec.val, Array.to_slice, Slice.from_val]
      have hv : (index_mut_back true).val = (List.replicate succ.length false).set source.val true := by
        simp [index_mut_back_post, visited_post, alloc.vec.Vec.set]
      apply path_reaches_join_loop_spec succ source join taken (index_mut_back true, _, 0#usize)
      refine ⟨⟨?_, ?_, ?_, ?_, ?_⟩, ?_, ?_, ?_⟩ <;> dsimp only <;> simp only [hq, hv]
      · simp
      · intro w hw
        by_cases h : w = source.val
        · subst h; simp [List.getD_eq_getElem?_getD, hw]
        · simp [List.getD_eq_getElem?_getD, List.getElem?_set_ne (Ne.symm h), h]
      · simpa using hn
      · simp
      · simpa using ⟨Relation.ReflTransGen.refl, hne'⟩
      · simp
      · simp
      · intro k hk; simp at hk

/-- `has_active_path`: some live source other than the join reaches the join
over open edges. -/
@[step]
theorem has_active_path_spec (succ : Slice (alloc.vec.Vec (Usize × Usize)))
    (live : Slice Usize) (join : Usize) (taken : Slice U32) :
    has_active_path succ live join taken ⦃ b =>
      (b = true ↔ ∃ m ∈ live.val.map (·.val), m ≠ join.val ∧
        KReach succ taken join.val m join.val) ⦄ := by
  unfold has_active_path has_active_path_loop
  apply loop.spec_decr_nat (fun (i : Usize) => live.length - i.val)
    (fun i => i.val ≤ live.length ∧ ∀ k (_ : k < i.val) (hk : k < live.length),
      (live.val[k]'(by simpa using hk)).val ≠ join.val →
        ¬ KReach succ taken join.val (live.val[k]'(by simpa using hk)).val join.val)
  · rintro i ⟨hi, hdone⟩
    unfold has_active_path_loop.body
    dsimp only
    split
    · rename_i hlt
      step as ⟨m, m_post⟩
      have hik : i.val < live.length := by scalar_tac
      split
      · rename_i hne
        have hne' : m.val ≠ join.val := by
          intro e; simp [UScalar.eq_of_val_eq e] at hne
        step as ⟨b, b_post⟩
        split
        · rename_i hb
          apply (WP.spec_ok _).mpr
          refine ⟨fun _ => ⟨m.val, ?_, hne', b_post.mp hb⟩, fun _ => rfl⟩
          rw [m_post]; exact List.mem_map_of_mem (List.getElem_mem _)
        · rename_i hb
          step as ⟨i3, i3_post⟩
          refine ⟨by scalar_tac, ?_, by scalar_tac⟩
          intro k hk hk' hkne
          rcases Nat.lt_succ_iff_lt_or_eq.mp (i3_post ▸ hk) with hk | rfl
          · exact hdone k hk hk' hkne
          · simp only [← m_post]
            exact fun h => hb (b_post.mpr h)
      · rename_i heq
        step as ⟨i3, i3_post⟩
        refine ⟨by scalar_tac, ?_, by scalar_tac⟩
        intro k hk hk' hkne
        rcases Nat.lt_succ_iff_lt_or_eq.mp (i3_post ▸ hk) with hk | rfl
        · exact hdone k hk hk' hkne
        · simp only [← m_post] at hkne
          simp at heq
          exact absurd heq hkne
    · rename_i hge
      apply (WP.spec_ok _).mpr
      have hie : i.val = live.length := by scalar_tac
      simp only [Bool.false_eq_true, false_iff, not_exists, not_and]
      intro m hm hne hr
      obtain ⟨k, hk, hkv⟩ := List.mem_iff_getElem.mp hm
      have hk2 : k < live.length := by simpa using hk
      simp only [List.getElem_map] at hkv
      exact hdone k (by omega) hk2 (hkv ▸ hne) (hkv ▸ hr)
  · exact ⟨by simp, fun k hk => absurd hk (by simp)⟩

/-! ## Refinement: the kernel's join guard is `ZeebeJoinReady` -/

/-- Under one-target flows, the kernel's open edges are exactly Zeebe's
unblocked ones: an edge into a node other than the join can never carry one of
the join's incoming flows. -/
theorem kstep_iff (succ : Slice (alloc.vec.Vec (Usize × Usize))) (taken : Slice U32) (j : Nat)
    (hone : Zeebe.FlowsHaveOneTarget (graph succ) taken.length) (u v : Nat) :
    KStep succ taken j u v ↔ Zeebe.Step (graph succ) taken.length (takenN taken) j u v := by
  constructor
  · rintro ⟨f, hm, ho⟩
    refine ⟨f, hm, ?_⟩
    rintro ⟨⟨hf, u', hm'⟩, hpos⟩
    rcases ho with hv | hz
    · exact hv (hone u u' v j f hf hm hm')
    · omega
  · rintro ⟨f, hm, hnb⟩
    refine ⟨f, hm, ?_⟩
    by_cases hv : v = j
    · subst hv
      right
      by_cases hf : f < taken.length
      · by_contra hz
        exact hnb ⟨⟨hf, u, hm⟩, by omega⟩
      · exact takenN_of_ge (by omega)
    · exact Or.inl hv

theorem kreach_iff (succ : Slice (alloc.vec.Vec (Usize × Usize))) (taken : Slice U32) (j s : Nat)
    (hone : Zeebe.FlowsHaveOneTarget (graph succ) taken.length) :
    KReach succ taken j s j ↔ Zeebe.PathReaches (graph succ) taken.length (takenN taken) s j := by
  have : KStep succ taken j = Zeebe.Step (graph succ) taken.length (takenN taken) j := by
    funext u v
    exact propext (kstep_iff succ taken j hone u v)
  unfold KReach Zeebe.PathReaches
  rw [this]

theorem saturating_add_zero (x : Usize) : core.num.Usize.saturating_add x 0#usize = x := by
  apply UScalar.eq_of_val_eq
  have h := x.hmax
  simp only [core.num.Usize.saturating_add, UScalar.saturating_add, UScalar.val, UScalar.max]
  simp at *
  rw [Nat.min_eq_right (by omega), Nat.mod_eq_of_lt h]

/-- The join's kind, as the TLA+ module names it. -/
def kindN : JoinKind → Zeebe.Kind
  | .Parallel => .parallel
  | .Inclusive => .inclusive

/-- **The refinement theorem.** For every graph whose sequence flows have one
target, and every `incoming` listing exactly the join's incoming sequence
flows, the extracted `join_ready` kernel terminates without error and accepts
exactly when Zeebe's join guard `ZeebeJoinReady` holds. `unidentified` is a
Nano-only input (arrivals recorded without a flow id) that Zeebe does not
have, so it is zero here; it is covered by the Rust tests. -/
theorem join_ready_refines (succ : Slice (alloc.vec.Vec (Usize × Usize))) (kind : JoinKind)
    (join : Usize) (incoming live : Slice Usize) (taken : Slice U32)
    (hone : Zeebe.FlowsHaveOneTarget (graph succ) taken.length)
    (hin : ∀ f, f ∈ incoming.val.map (·.val) ↔ Zeebe.In (graph succ) taken.length join.val f) :
    join_ready succ kind join incoming live taken 0#usize ⦃ b =>
      (b = true ↔ Zeebe.JoinReady (graph succ) taken.length (takenN taken) (kindN kind) join.val
        (live.val.map (·.val))) ⦄ := by
  have hall : Zeebe.ParallelReady (graph succ) taken.length (takenN taken) join.val ↔
      ∀ x ∈ incoming.val, 0 < takenN taken x.val := by
    constructor
    · intro h x hx
      exact h x.val ((hin x.val).mp (List.mem_map_of_mem hx))
    · intro h f hf
      obtain ⟨x, hx, rfl⟩ := List.mem_map.mp ((hin f).mpr hf)
      exact h x hx
  have hany : (∃ f, Zeebe.In (graph succ) taken.length join.val f ∧ 0 < takenN taken f) ↔
      ∃ x ∈ incoming.val, 0 < takenN taken x.val := by
    constructor
    · rintro ⟨f, hf, hp⟩
      obtain ⟨x, hx, rfl⟩ := List.mem_map.mp ((hin f).mpr hf)
      exact ⟨x, hx, hp⟩
    · rintro ⟨x, hx, hp⟩
      exact ⟨x.val, (hin x.val).mp (List.mem_map_of_mem hx), hp⟩
  unfold join_ready
  step as ⟨c, c_post⟩
  simp only [lift, saturating_add_zero, bind_ok]
  have hle : c.val ≤ incoming.length := by
    rw [c_post]; exact List.length_filter_le _ _
  have hfull : c.val ≥ incoming.length ↔ ∀ x ∈ incoming.val, 0 < takenN taken x.val := by
    have hl : c.val ≥ incoming.length ↔ c.val = incoming.val.length := by
      simp only [Slice.length] at hle ⊢; omega
    rw [hl, c_post, List.length_filter_eq_length_iff]
    simp
  have hpos : 0 < c.val ↔ ∃ x ∈ incoming.val, 0 < takenN taken x.val := by
    rw [c_post, List.length_pos_iff_exists_mem]
    simp
  unfold join_decision
  split
  · rename_i hge
    have hge' : c.val ≥ incoming.length := by scalar_tac
    simp only [bind_ok]
    apply (WP.spec_ok _).mpr
    simp only [true_iff]
    cases kind
    · exact hall.mpr (hfull.mp hge')
    · exact Or.inl (hall.mpr (hfull.mp hge'))
  · rename_i hlt
    have hnot : ¬ Zeebe.ParallelReady (graph succ) taken.length (takenN taken) join.val :=
      fun h => hlt (by have := hfull.mpr (hall.mp h); scalar_tac)
    cases kind
    · simp only [bind_ok]
      apply (WP.spec_ok _).mpr
      simp only [Bool.false_eq_true, false_iff, kindN, Zeebe.JoinReady]
      exact hnot
    · dsimp only
      split
      · rename_i hgt
        have hany' := hpos.mp (by scalar_tac)
        simp only [bind_ok]
        step as ⟨b, b_post⟩
        simp only [kindN, Zeebe.JoinReady, Zeebe.InclusiveReady, Zeebe.HasActivePath,
          decide_eq_true_eq]
        rw [b_post]
        constructor
        · intro h
          right
          exact ⟨hany.mpr hany', fun ⟨m, hm, hne, hr⟩ =>
            h ⟨m, hm, hne, (kreach_iff succ taken join.val m hone).mpr hr⟩⟩
        · rintro (h | ⟨_, h⟩) ⟨m, hm, hne, hr⟩
          · exact hnot h
          · exact h ⟨m, hm, hne, (kreach_iff succ taken join.val m hone).mp hr⟩
      · rename_i hz
        simp only [bind_ok]
        apply (WP.spec_ok _).mpr
        simp only [Bool.false_eq_true, false_iff, kindN, Zeebe.JoinReady, Zeebe.InclusiveReady,
          not_or, not_and]
        refine ⟨hnot, fun h => absurd (hpos.mpr (hany.mp h)) (by scalar_tac)⟩

end Refinement
