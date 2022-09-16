# Python クライアント SDK

_他言語バージョンもあります_：[English](README.md)

<br>

## SDK コード例を実行する

- [Python 3.7 もしくはそれ以上が必要です。](https://www.python.org/downloads/)
- Momento オーストークンが必要です。トークン発行は[Momento CLI](https://github.com/momentohq/momento-cli)から行えます。

```bash
python3 -m pip install --user pipenv
pipenv install
```

```bash
MOMENTO_AUTH_TOKEN=<YOUR_TOKEN> pipenv run python example.py
MOMENTO_AUTH_TOKEN=<YOUR_TOKEN> pipenv run python example_async.py
```

SDK のデバッグログをオンするには、下記のように実行して下さい:

```bash
DEBUG=true MOMENTO_AUTH_TOKEN=<YOUR_TOKEN> pipenv run python example.py
DEBUG=true MOMENTO_AUTH_TOKEN=<YOUR_TOKEN> pipenv run python example_async.py
```

## SDK を自身のプロジェクトで使用する

```bash
pipenv install momento==0.14.0
```

or

`momento==0.14.0`を`requirements.txt`に追加する、もしくは自身のプロジェクで使用しているディペンデンシー管理フレームワークに追加して下さい。

自身のシステムに直接インストールする方法:

```bash
pip install momento==0.14.0
```
