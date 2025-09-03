import json
import os

def validate_credentials_file(file_path='credentials.json'):
    """
    credentials.jsonファイルの形式を検証し、問題を特定する
    """
    print("=== credentials.json ファイル検証ツール ===\n")
    
    # ファイルの存在確認
    if not os.path.exists(file_path):
        print(f"❌ エラー: ファイル '{file_path}' が見つかりません。")
        print("\n📋 解決手順:")
        print("1. Google Cloud Console にアクセス")
        print("2. 「APIとサービス」→「認証情報」")
        print("3. サービスアカウントを選択")
        print("4. 「キー」タブ → 「キーを追加」→「新しいキーを作成」")
        print("5. 「JSON」形式を選択してダウンロード")
        print("6. ダウンロードしたファイルを 'credentials.json' にリネーム")
        return False
    
    # ファイルサイズ確認
    file_size = os.path.getsize(file_path)
    print(f"📁 ファイルサイズ: {file_size} bytes")
    
    if file_size == 0:
        print("❌ エラー: ファイルが空です。")
        return False
    
    if file_size < 100:
        print("⚠️  警告: ファイルサイズが小さすぎます。正しいファイルか確認してください。")
    
    try:
        # JSONファイルの読み込み
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
        print(f"📄 ファイル内容（最初の200文字）:")
        print(f"   {content[:200]}...")
        print()
        
        # JSON形式の検証
        try:
            credentials_data = json.loads(content)
        except json.JSONDecodeError as e:
            print(f"❌ JSON形式エラー: {e}")
            print("\n🔧 修正方法:")
            print("1. ファイルを再ダウンロードしてください")
            print("2. テキストエディタで開いて、文字化けがないか確認")
            print("3. ファイルの最初と最後が { と } で囲まれているか確認")
            return False
        
        print("✅ JSON形式: 正常")
        
        # 必須フィールドの確認
        required_fields = [
            'type',
            'project_id', 
            'private_key_id',
            'private_key',
            'client_email',
            'client_id',
            'auth_uri',
            'token_uri',
            'auth_provider_x509_cert_url',
            'client_x509_cert_url'
        ]
        
        missing_fields = []
        present_fields = []
        
        print("📋 必須フィールドの確認:")
        for field in required_fields:
            if field in credentials_data and credentials_data[field]:
                present_fields.append(field)
                # 重要な情報は一部のみ表示
                if field == 'client_email':
                    print(f"   ✅ {field}: {credentials_data[field]}")
                elif field == 'project_id':
                    print(f"   ✅ {field}: {credentials_data[field]}")
                elif field == 'type':
                    print(f"   ✅ {field}: {credentials_data[field]}")
                else:
                    print(f"   ✅ {field}: [存在]")
            else:
                missing_fields.append(field)
                print(f"   ❌ {field}: [欠落]")
        
        # サービスアカウントタイプの確認
        if credentials_data.get('type') != 'service_account':
            print(f"\n⚠️  警告: type フィールドが 'service_account' ではありません: {credentials_data.get('type')}")
            print("   OAuth認証情報ではなく、サービスアカウントキーをダウンロードする必要があります。")
        
        # 結果のまとめ
        print(f"\n📊 検証結果:")
        print(f"   ✅ 存在するフィールド: {len(present_fields)}/{len(required_fields)}")
        print(f"   ❌ 欠落フィールド: {len(missing_fields)}")
        
        if missing_fields:
            print(f"\n❌ 欠落しているフィールド: {', '.join(missing_fields)}")
            print("\n🔧 解決方法:")
            print("1. Google Cloud Console から正しいサービスアカウントキーを再ダウンロード")
            print("2. 「OAuth 2.0 クライアント ID」ではなく「サービスアカウント」を選択")
            print("3. キー形式は「JSON」を選択")
            return False
        else:
            print("\n✅ すべての必須フィールドが存在します！")
            return True
            
    except FileNotFoundError:
        print(f"❌ ファイル '{file_path}' を読み込めません。")
        return False
    except Exception as e:
        print(f"❌ 予期しないエラー: {e}")
        return False

def create_sample_credentials():
    """
    正しい credentials.json の構造例を表示
    """
    sample = {
        "type": "service_account",
        "project_id": "your-project-id",
        "private_key_id": "key-id",
        "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
        "client_email": "your-service-account@your-project-id.iam.gserviceaccount.com",
        "client_id": "client-id",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/your-service-account%40your-project-id.iam.gserviceaccount.com"
    }
    
    print("\n📝 正しい credentials.json の構造例:")
    print(json.dumps(sample, indent=2, ensure_ascii=False))

def fix_common_issues():
    """
    よくある問題の修正方法を案内
    """
    print("\n🛠️  よくある問題と解決法:")
    print("\n1️⃣  「OAuth クライアント ID」をダウンロードした場合:")
    print("   → サービスアカウントキーを作成し直してください")
    print("   → Google Cloud Console → 認証情報 → 認証情報を作成 → サービスアカウント")
    
    print("\n2️⃣  ファイルが文字化けしている場合:")
    print("   → ブラウザから直接ダウンロードし直してください")
    print("   → ファイルをテキストエディタで開いて内容を確認")
    
    print("\n3️⃣  古い形式のキーファイルの場合:")
    print("   → 新しいサービスアカウントキーを作成してください")
    print("   → 既存のキーは削除して、新しいキーを生成")
    
    print("\n4️⃣  プロジェクトの設定問題:")
    print("   → Google Sheets API が有効化されているか確認")
    print("   → Google Drive API も有効化されているか確認")

if __name__ == "__main__":
    print("Google Sheets認証情報の検証を開始します...\n")
    
    # credentials.jsonファイルを検証
    is_valid = validate_credentials_file()
    
    if not is_valid:
        print("\n" + "="*60)
        create_sample_credentials()
        fix_common_issues()
        
        print("\n🔗 参考リンク:")
        print("   Google Cloud Console: https://console.cloud.google.com/")
        print("   サービスアカウント作成ガイド: https://cloud.google.com/iam/docs/creating-managing-service-account-keys")
    else:
        print("\n🎉 credentials.json は正常です！")
        print("   Google Sheets クライアントの初期化が可能です。")