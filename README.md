# nicoarch-worker

[nicoarch](https://github.com/tksnnx/nicoarch.git)<br/>
[nicoarch-app](https://github.com/tksnnx/nicoarch-app.git)

ニコニコ動画のアーカイブツールnicoarchのワーカー部分

## 使い方

### production

1. [nicoarch](https://github.com/tksnnx/nicoarch.git)の`docker-compose.yml`を用いてツールを起動する。

### development

1. [nicoarch](https://github.com/tksnnx/nicoarch.git)の`docker-compose.dev.yml`を用いて
    redisとmongoサーバーを起動する。
2. `.env.sample`を`.env`にコピーし、編集する。
3. 下記コマンドを実行し、開発サーバーを起動する。
    ```sh
    poetry install
    poetry run python -r src/worker.py
    ```

## ライセンス

[MIT License](LICENSE)
