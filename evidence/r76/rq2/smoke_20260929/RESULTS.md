# R76 B3 results: matched-span surface vs latent (R73 pool programs)

Saving = 100*(1 - latent/surface), complete archive bytes (semantic + shared residual + manifest).
Held-out = blocks 1.. (sampled files: the 19 non-zero sampled blocks). Residual archive is byte-identical in both arms.

| Dataset | Scope | Blocks | Surface B | Latent B | Saving % | Held-out % | Sem. surface B | Sem. latent B | Residual B | Program streams replaced | Blocks latent smaller / larger | Latent = R73 main (blocks) | Archive-only decode |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Apache | complete | 1/1 | 82026 | 69646 | 15.09 | n/a (1 block) | 64640 | 52260 | 17232 | G(open_function), H(open_function), I(open_function), X4TS(open_function) | 1 / 0 | 1/1 | PASS |
| Linux | complete | 1/1 | 63725 | 58613 | 8.02 | n/a (1 block) | 34108 | 28996 | 29464 | H(open_function), PD(open_function), TS(open_function) | 1 / 0 | 1/1 | PASS |
| Proxifier | complete | 1/1 | 79389 | 70361 | 11.37 | n/a (1 block) | 74072 | 65044 | 5160 | BR(open_function), BS(open_function), PT(open_function), X0DU(open_function), X3TS(open_function) | 1 / 0 | 1/1 | PASS |

Complete files (3): surface 225140 B, latent 198620 B, total saving 11.78%.
