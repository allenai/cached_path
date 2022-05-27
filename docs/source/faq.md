FAQ
===

## Can I disable the progress bars/displays?

Yeup! Just set the argument `quiet=True` when you call {func}`~cached_path.cached_path()`.

## Does `cached_path` have an async interface?

No, not at the moment. But you can still benefit by calling {func}`~cached_path.cached_path()` concurrently
using {func}`asyncio.to_thread()`. For example:

```{testsetup}
>>> import sys, pytest, asyncio
>>> from cached_path import cached_path, get_unsized_download_progress
>>> if sys.version_info < (3, 9):
...     pytest.skip("This doctest requires Python >= 3.9")
>>>
```

```python
>>> async def main():
...     print("Downloading files...")
...     progress = get_unsized_download_progress()
...     await asyncio.gather(
...         asyncio.to_thread(
...             cached_path,
...             "https://github.com/allenai/cached_path/blob/main/README.md",
...             progress=progress,
...         ),
...         asyncio.to_thread(
...             cached_path,
...             "https://github.com/allenai/cached_path/blob/main/setup.py",
...             progress=progress,
...         ),
...         asyncio.to_thread(
...             cached_path,
...             "https://github.com/allenai/cached_path/blob/main/requirements.txt",
...             progress=progress,
...         ),
...     )
...     print("Finished all downloads")
...
>>> asyncio.run(main())
Downloading files...
Finished all downloads
>>>
```
