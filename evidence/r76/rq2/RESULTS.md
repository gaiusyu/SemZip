# R76 B3 results: matched-span surface vs latent (R73 pool programs)

Saving = 100*(1 - latent/surface), complete archive bytes (semantic + shared residual + manifest).
Held-out = blocks 1.. (sampled files: the 19 non-zero sampled blocks). Residual archive is byte-identical in both arms.

| Dataset | Scope | Blocks | Surface B | Latent B | Saving % | Held-out % | Sem. surface B | Sem. latent B | Residual B | Program streams replaced | Blocks latent smaller / larger | Latent = R73 main (blocks) | Archive-only decode |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Android | complete | 16/16 | 5602902 | 5565790 | 0.66 | 0.68 | 561764 | 524652 | 5040532 | TS1(open_function) | 16 / 0 | 16/16 | PASS |
| Apache | complete | 1/1 | 82026 | 69646 | 15.09 | n/a (1 block) | 64640 | 52260 | 17232 | G(open_function), H(open_function), I(open_function), X4TS(open_function) | 1 / 0 | 1/1 | PASS |
| BGL | complete | 48/48 | 14680374 | 14203694 | 3.25 | 3.28 | 11943360 | 11466680 | 2735452 | TS(open_function) | 41 / 7 | 48/48 | PASS |
| Hadoop | complete | 4/4 | 633820 | 571500 | 9.83 | 9.15 | 269080 | 206760 | 364496 | H(open_function), TS0(open_function) | 4 / 0 | 4/4 | PASS |
| HealthApp | complete | 3/3 | 433917 | 395273 | 8.91 | 8.57 | 234172 | 195528 | 199528 | TS(open_function) | 3 / 0 | 3/3 | PASS |
| HPC | complete | 5/5 | 732843 | 732843 | 0.00 | 0.00 | 162656 | 162656 | 569916 | none | 0 / 0 | 5/5 | PASS |
| Linux | complete | 1/1 | 63725 | 58613 | 8.02 | n/a (1 block) | 34108 | 28996 | 29464 | H(open_function), PD(open_function), TS(open_function) | 1 / 0 | 1/1 | PASS |
| Mac | complete | 2/2 | 313549 | 311853 | 0.54 | 0.25 | 65204 | 63508 | 248164 | TS(open_function), X2PT(open_context_delta), X2TS0(open_function), X3TS0(open_function) | 2 / 0 | 2/2 | PASS |
| OpenSSH | complete | 7/7 | 728835 | 623323 | 14.48 | 12.70 | 659096 | 553584 | 69404 | PT(open_context_delta), US(open_function), X2PD(open_function), X2TS(open_function) | 6 / 1 | 7/7 | PASS |
| OpenStack | complete | 3/3 | 2481449 | 2434257 | 1.90 | 2.02 | 2113416 | 2066224 | 367816 | X2A(open_function), X2TS(open_function) | 3 / 0 | 3/3 | PASS |
| Proxifier | complete | 1/1 | 79389 | 70361 | 11.37 | n/a (1 block) | 74072 | 65044 | 5160 | BR(open_function), BS(open_function), PT(open_function), X0DU(open_function), X3TS(open_function) | 1 / 0 | 1/1 | PASS |
| Zookeeper | complete | 1/1 | 63013 | 55045 | 12.65 | n/a (1 block) | 50564 | 42596 | 12292 | TS0(open_function), X4DU(open_function) | 1 / 0 | 1/1 | PASS |
| HDFS | sample20 | 20/112 | 10051953 | 10051953 | 0.00 | 0.00 | 7589436 | 7589436 | 2461716 | none | 0 / 0 | 20/20 | PASS |
| Spark | sample20 | 20/333 | 2887699 | 2876067 | 0.40 | 0.13 | 295504 | 283872 | 2591380 | G(open_function), K(open_function), L(open_function), U(open_function), V(open_function), W(open_function), X3TS(open_function), Y(open_function) | 14 / 3 | 20/20 | PASS |
| Windows | sample20 | 20/1147 | 767784 | 773208 | -0.71 | -0.78 | 583812 | 589236 | 183148 | D(open_function), TS0(open_function) | 1 / 19 | 20/20 | PASS |
| Thunderbird | sample20 | 20/2113 | 3911949 | 3911949 | 0.00 | 0.00 | 216980 | 216980 | 3694132 | none | 0 / 0 | 20/20 | PASS |

Complete files (12): surface 25895842 B, latent 25092198 B, total saving 3.10%.
Sampled files (4 x 20 blocks): surface 17619385 B, latent 17613177 B, total saving 0.04%.
