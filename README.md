# Converter from Git to Pijul

Just apply it like so:

```
mkdir repo
cd repo
git init
echo a > a
git commit -a -m "test"
cd ..

git-pijul repo
```
