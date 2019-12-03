#### Generating BOTE plots

Have in `out.dat` several lines that look like this one:

```
[n=3] a1=(330, 0.19, 52.91) f1=(442, 0.21, 75.15) e=(330, 0.19, 52.91) | [n=5] a1=(294, 0.20, 48.20) f1=(424, 0.23, 72.49) a2=(329, 0.16, 46.48) f2=(452, 0.21, 72.49) e=(294, 0.20, 48.20) | [n=7] a1=(278, 0.19, 44.66) f1=(420, 0.23, 72.49) a2=(289, 0.17, 43.18) f2=(424, 0.23, 72.49) e=(289, 0.17, 43.18) | [n=9] a1=(262, 0.16, 34.85) f1=(412, 0.23, 72.49) a2=(274, 0.15, 33.60) f2=(420, 0.23, 72.49) e=(274, 0.15, 33.60) | [n=11] a1=(187, 0.20, 32.63) f1=(381, 0.25, 72.49) a2=(232, 0.20, 34.08) f2=(412, 0.23, 72.49) e=(248, 0.19, 36.77) | [n=13] a1=(163, 0.26, 33.15) f1=(327, 0.29, 72.49) a2=(191, 0.24, 37.59) f2=(339, 0.28, 72.49) e=(222, 0.25, 43.18) | [n=15] a1=(147, 0.29, 32.93) f1=(327, 0.29, 72.49) a2=(162, 0.27, 35.12) f2=(339, 0.28, 72.49) e=(218, 0.28, 47.88) |
```

and then

```bash
$ python3 plot.py
```

This will generate one `png` per line:
- if there are three lines, there will be three `png` files: `0.png`, `1.png` and `2.png`