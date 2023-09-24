# 東京都の卸売市場のデータを抽出します
データ分析を勉強したいと思い、東京都の卸売市場のデータを抽出しました。
データは、東京都のオープンデータを利用させていただきました。
https://www.shijou.metro.tokyo.lg.jp/torihiki/week/

## 環境
* Windows 11 Pro
* Visual Studio Code
* Python 3.9.7
* Azure Functions Core Tools 3.0.3568

## データの抽出方法
データの抽出方法は、スクレイピングを利用しています。
Azure Functionsでスクレイピングを実行し、Azure Blob Storageにデータを保存しています。
実行にはタイマー方式と、HTTPリクエスト方式の2種類があります。
※

## ローカルでの実行方法
1. 「local.settubgs.json」を作成しAzureWebJobsStorageに接続文字列を設定してください。
2. .venvを作成します
```powershell
py -m venv .venv
```
3. デバッグを実行します

## 注意事項
スクレイピングの際に、robots.txtを確認してください。
```bash
curl https://www.shijou.metro.tokyo.lg.jp/robots.txt
```