# Topology verification summary

## Dataset scope

- Assets in metadata: **43**
- Assets present in Gold layer (ALL phase): **43**
- Rows analysed: **1,039,873**
- Window range: **2024-12-31 07:30:00** to **2025-12-31 23:45:00**

## Declared parent-child containment

- Relationships tested: **66**
- Zero hard-violation relationships: **21**
- Relationships with hard-violation rate ≤ 0.1%: **23**

### Highest declared parent-child violation rates

| Parent | Child | Overlap windows | Hard violations | Hard violation rate (%) | Corr |
|---|---:|---:|---:|---:|---:|
| sb_c | sb_a | 21164 | 21164 | 100.000 | 0.288 |
| sb_c | pr_k | 4164 | 4164 | 100.000 | -0.003 |
| sb_c | pr_f | 3968 | 3968 | 100.000 | 0.324 |
| sb_c | p_j | 1312 | 1312 | 100.000 | 0.256 |
| sb_c | pr_e | 4067 | 4066 | 99.975 | 0.241 |
| sb_c | p_m | 23336 | 23224 | 99.520 | 0.065 |
| sb_c | p_d | 23336 | 22980 | 98.474 | -0.051 |
| sb_c | p_c | 21301 | 20766 | 97.488 | -0.019 |
| sb_c | sb_b | 21095 | 20365 | 96.539 | 0.042 |
| sb_c | p_a | 23336 | 22417 | 96.062 | -0.040 |

## Declared parent-group closure

- Parent groups tested: **5**
- Parent groups with zero hard violations: **0**

### Highest parent-group closure violation rates

| Parent | Present children | Overlap windows | Hard violations | Hard violation rate (%) |
|---|---:|---:|---:|---:|
| sb_c | 26 | 1202 | 1202 | 100.000 |
| mi_b | 3 | 23336 | 14620 | 62.650 |
| sb_b | 8 | 21095 | 736 | 3.489 |
| sb_a | 6 | 21164 | 688 | 3.251 |
| mi_a | 23 | 1202 | 1 | 0.083 |

## Probable parent rankings

| Child | Best inferred parent | Corr | Hard violation rate (%) | Parent dominance (%) | Score |
|---|---:|---:|---:|---:|---:|
| u_e | sb_b | 0.764 | 3.470 | 96.53 | 0.717 |
| sb_a | mi_a | 0.712 | 1.489 | 98.51 | 0.700 |
| ex_a | sb_a | 0.684 | 0.000 | 100.00 | 0.694 |
| u_b | sb_a | 0.678 | 0.000 | 100.00 | 0.690 |
| hp_b | sb_c | 0.744 | 7.769 | 92.23 | 0.668 |
| sb_b | sb_a | 0.645 | 4.129 | 95.87 | 0.634 |
| pr_f | mi_a | 0.589 | 0.000 | 100.00 | 0.633 |
| mh_a | sb_a | 0.564 | 0.000 | 100.00 | 0.617 |
| ahu_c | sb_a | 0.556 | 0.000 | 100.00 | 0.612 |
| ahu_b | mi_a | 0.500 | 0.000 | 100.00 | 0.575 |
| ex_b | mi_a | 0.481 | 0.255 | 99.74 | 0.561 |
| mix_a | sb_b | 0.513 | 3.465 | 96.53 | 0.554 |
| pr_k | mi_a | 0.446 | 0.000 | 100.00 | 0.540 |
| mix_b | sb_b | 0.471 | 3.461 | 96.54 | 0.527 |
| sb_c | mi_a | 0.423 | 0.000 | 100.00 | 0.525 |
| php_c | mi_a | 0.418 | 0.000 | 100.00 | 0.522 |
| p_f | mi_a | 0.398 | 0.085 | 99.91 | 0.508 |
| mix_c | sb_b | 0.413 | 1.555 | 98.45 | 0.505 |
| pr_e | mi_a | 0.366 | 0.000 | 100.00 | 0.488 |
| p_k | mi_a | 0.361 | 0.364 | 99.64 | 0.482 |

## Interpretation note

These tests provide statistical support or contradiction for an assumed monitoring hierarchy, but they do not prove electrical topology in the strict sense. In particular, magnitude-only Gold-layer data cannot resolve reverse power flow or export direction.
