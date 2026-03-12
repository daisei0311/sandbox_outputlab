# %%
import pandas as pd
from pathlib import Path
import ibis
import os
import sys
import calendar
from datetime import datetime
from dateutil.relativedelta import relativedelta

project_root = os.path.dirname(Path(__file__).resolve().parent)

# sys.pathにプロジェクトルートを追加
sys.path.append(project_root)
import util.tm_utility as tu  # noqa: E402


pd.set_option("display.max_columns", 200)  # 列数の上限を解除
base_dir = Path(__file__).resolve().parent.parent
CONFIG_FILE = base_dir / "util" / "config_2.yaml"

print("================設定値読み込み================================")
goki_extract = "2"
lookback_months = 4  # 「直近何ヶ月分」を取得するかを指定（3なら今月を含む過去3ヶ月）

C_A = "正"
pg_cfg_eq = tu.load_connection_config(
    path=CONFIG_FILE, db_yaml_config="pg_connection_config_eqlog"
)
pg_cfg_agg = tu.load_connection_config(
    path=CONFIG_FILE, db_yaml_config="pg_connection_config_agg"
)
con_pg_agg = ibis.postgres.connect(**pg_cfg_agg)
con_eq = ibis.postgres.connect(**pg_cfg_eq)
read_cfg = tu.load_connection_config(
    path=CONFIG_FILE, db_yaml_config="atumi_huryo_maisu_cnt_cfg"
)

# head_id = read_cfg["head_id"]
st_id1 = read_cfg["st_id1"]
st_id2 = read_cfg["st_id2"]
atumi_ave_line_col = [f"変位計{i}平均" for i in range(1, 6)]
# 厚み平均算出
thickness_columns = [
    f"変位計{i}_{j:02d}_{j + 1:02d}平均"
    for j in range(1, 20, 2)
    for i in range(1, 6, 1)
]
hennikei_hantei_cols = [f"変位計{i + 1}_判定" for i in range(5)]
cols_ave_kairi = [col + "_乖離率" for col in thickness_columns]
cols_ave_hantei = [col + "_判定" for col in thickness_columns]


# 実行した瞬間の「現在の日時」を取得
now = datetime.now()

# ==========================================
# 自動期間生成ループ
# ==========================================
# reversed() を使うことで、古い月(i=2) -> 新しい月(i=0) の「時系列順」で処理します
for i in reversed(range(lookback_months)):
    # 現在から i ヶ月前の日付を正確に計算（年またぎも自動で処理されます）
    target_date = now - relativedelta(months=i)
    target_year = target_date.year
    target_month = target_date.month

    # calendar.monthrange を使って指定した月の最終日を自動取得
    _, last_day = calendar.monthrange(target_year, target_month)

    # Pythonのdatetimeオブジェクトとして初日と末日を作成
    start_time = datetime(target_year, target_month, 1, 0, 0, 0)
    end_time = datetime(target_year, target_month, last_day, 23, 59, 59)

    # 文字列（YYYY-MM-DD HH:MM:SS）としてIbis等に渡す場合はここで変換
    start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
    end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S")

    print(f"処理対象: {start_time_str} 〜 {end_time_str}")
    # =========================================================
    # STEP 1: log0 (軽いテーブル) から1日100枚のIDをランダムサンプリング
    # =========================================================
    print(
        f"=================={C_A}極_箔id_caid(log0)読み込み============================"
    )
    table_union = None

    for goki in range(int(goki_extract), int(goki_extract) + 1, 1):
        TABLE_CA_HAKU_ID = f"uc{goki}_{C_A}極成形plc_log0"
        table_org = con_eq.table(TABLE_CA_HAKU_ID)[
            "date_time", "箔id", "ｶｰﾄﾘｯｼﾞid_rfid_", "下ﾍｯﾄﾞid", "下ﾍｯﾄﾞｼｮｯﾄ数", "ｽﾃﾝｼﾙid"
        ]
        if table_union is None:
            table_union = table_org
        else:
            table_union = table_union.union(table_org)

    # リネームと期間フィルター
    table_caid = table_union.rename(
        {"caid_rfid": "ｶｰﾄﾘｯｼﾞid_rfid_", "date_time_seikeilog0": "date_time"}
    )
    table_caid = table_caid.filter(
        (table_caid["date_time_seikeilog0"] >= ibis.literal(start_time))
        & (table_caid["date_time_seikeilog0"] <= ibis.literal(end_time))
    )

    # 【ポイント】DBから log0 の必要な列（日付と箔id）だけをサクッと取得
    print("log0からサンプリング用のIDリストを取得中...")
    df_ids = table_caid.select(["date_time_seikeilog0", "箔id"]).execute()

    # Pandasで各日100枚をランダムサンプリング
    df_ids["date"] = pd.to_datetime(df_ids["date_time_seikeilog0"]).dt.date
    df_sampled = (
        df_ids.groupby("date")
        .apply(lambda x: x.sample(n=min(len(x), 2000), random_state=42))
        .reset_index(drop=True)
    )

    # 抽選されたワークIDのリストを作成
    selected_wafers = df_sampled["箔id"].tolist()
    print(
        f"サンプリング完了: 計 {len(selected_wafers)} 枚のデータをlog3から抽出します。"
    )
    print(f"=================={C_A}極_成形log3読み込み============================")
    table_hyomen = f"uc{goki_extract}_{C_A}極成形plc_log3"

    t = con_eq.table(table_hyomen)
    # ==========================================
    # 抽出する必須カラムのリスト定義
    # ==========================================
    keep_cols = [
        # --- キー情報（groupbyで1枚の塗工を束ねるため） ---
        "date_time",
        "ﾜｰｸid情報",  # ウェハー/電極1枚（1ショット）を特定するID
        # --- Tier 2 & ノイズキャンセラー（1ショット内で一定の設定・環境値） ---
        "成形枚数",  # 摩耗の最重要ベースライン
        "成形方向_1_外中_2_中外_3_外中1枚目_",
        "ｽﾗﾘｰ粘度",  # 神パラメータ（粘度によるトルク変動を補正）
        # --- Tier 1: 波形データ（ここからAvg, Max, Min, Stdを作る源泉） ---
        "成形ﾍｯﾄﾞ1_成形開始基準相対位置",
        "成形ﾃｰﾌﾞﾙ1_現在位置_um_",
        "成形ﾃｰﾌﾞﾙ1_指令位置_um_",
        "成形ﾃｰﾌﾞﾙ1_現在速度_um_s_",
        "成形ﾋﾟｽﾄﾝ_圧力目標値_kpa_",
        "成形ﾋﾟｽﾄﾝ_圧力現在値_kpa_",
        "成形ﾋﾟｽﾄﾝ_現在位置_um_",
        "成形ﾋﾟｽﾄﾝ_現在速度_um_s_",
        "成形ﾋﾟｽﾄﾝ_帰還ﾄﾙｸ_0_01per_",  # 最重要：流路抵抗の変化（摩耗）がモロに出る
        "成形ﾍｯﾄﾞ1_帰還ﾄﾙｸ_0_01per_",
        "成形ﾍｯﾄﾞ2_帰還ﾄﾙｸ_0_01per_",
        "成形ﾍｯﾄﾞ1_現在速度_um_s_",
    ]

    # ==========================================
    # Ibisで列を絞り込む（この時点ではまだDBにクエリは飛ばない）
    # ==========================================
    t_selected = t.select(keep_cols).filter(
        (t["date_time"] >= ibis.literal(start_time))
        & (t["date_time"] <= ibis.literal(end_time))
    )
    t_selected = t_selected.select(keep_cols).filter(
        t_selected["ﾜｰｸid情報"].isin(selected_wafers)
    )
    t_featured = t_selected.mutate(
        # 現在値 - 目標値 を計算し、「圧力差分_kpa」という新しい列を作る
        ピストン圧力差分_kpa=t_selected["成形ﾋﾟｽﾄﾝ_圧力現在値_kpa_"].cast(float)
        - t_selected["成形ﾋﾟｽﾄﾝ_圧力目標値_kpa_"].cast(float)
    )
    t_featured = t_featured.mutate(
        # 現在値 - 目標値 を計算し、「圧力差分_kpa」という新しい列を作る
        テーブル位置差分_um=t_selected["成形ﾃｰﾌﾞﾙ1_現在位置_um_"].cast(float)
        - t_selected["成形ﾃｰﾌﾞﾙ1_指令位置_um_"].cast(float)
    )
    t_featured = t_featured.mutate(
        疑似流路抵抗=ibis.ifelse(
            t_selected["成形ﾋﾟｽﾄﾝ_現在速度_um_s_"].cast(float) == 0,
            0,
            t_selected["成形ﾋﾟｽﾄﾝ_圧力現在値_kpa_"].cast(float)
            / t_selected["成形ﾋﾟｽﾄﾝ_現在速度_um_s_"].cast(float),
        )
    )
    group_keys = [
        "ﾜｰｸid情報",
        "成形枚数",
        "成形方向_1_外中_2_中外_3_外中1枚目_",
        "ｽﾗﾘｰ粘度",
    ]
    STEADY_THRESHOLD = (
        129000  # 定常とみなす速度の閾値（現場の速度に合わせて調整してください）
    )

    # 条件1: 速度が閾値以上の区間を「定常(Steady)」とする
    cond_steady = (
        t_featured["成形ﾍｯﾄﾞ1_現在速度_um_s_"].cast(float).abs() >= STEADY_THRESHOLD
    )

    # 条件2: 速度が閾値未満、かつデータの「前半(data_no < 300)」なら「立上(Start)」
    cond_start = (
        t_featured["成形ﾍｯﾄﾞ1_現在速度_um_s_"].cast(float).abs() < STEADY_THRESHOLD
    ) & (t_featured["成形ﾍｯﾄﾞ1_成形開始基準相対位置"].cast(float) <= 50000)
    # 条件3: 速度が閾値未満、かつデータの「後半(data_no >= 300)」なら「立下(End)」
    cond_end = (
        t_featured["成形ﾍｯﾄﾞ1_現在速度_um_s_"].cast(float).abs() < STEADY_THRESHOLD
    ) & (t_featured["成形ﾍｯﾄﾞ1_成形開始基準相対位置"].cast(float) >= 250000)

    # 3. PostgreSQLに集約（Aggregate）計算を指示する
    t_agg = t_featured.group_by(group_keys).aggregate(
        # ------------------------------------------
        # 【1. 立上フェーズ (Start)】
        # ------------------------------------------
        立上_ﾄﾙｸ_avg=cond_start.ifelse(
            t_featured["成形ﾋﾟｽﾄﾝ_帰還ﾄﾙｸ_0_01per_"].cast(float), None
        ).mean(),
        立上_ﾄﾙｸ_max=cond_start.ifelse(
            t_featured["成形ﾋﾟｽﾄﾝ_帰還ﾄﾙｸ_0_01per_"].cast(float), None
        ).max(),
        立上_ﾄﾙｸ_min=cond_start.ifelse(
            t_featured["成形ﾋﾟｽﾄﾝ_帰還ﾄﾙｸ_0_01per_"].cast(float), None
        ).min(),
        立上_ﾄﾙｸ_std=cond_start.ifelse(
            t_featured["成形ﾋﾟｽﾄﾝ_帰還ﾄﾙｸ_0_01per_"].cast(float), None
        ).std(),
        立上_ピストン差分_max=cond_start.ifelse(
            t_featured["ピストン圧力差分_kpa"].cast(float), None
        ).max(),
        立上_ピストン差分_min=cond_start.ifelse(
            t_featured["ピストン圧力差分_kpa"].cast(float), None
        ).min(),
        立上_ピストン差分_std=cond_start.ifelse(
            t_featured["ピストン圧力差分_kpa"].cast(float), None
        ).std(),
        立上_ピストン現在_max=cond_start.ifelse(
            t_featured["成形ﾋﾟｽﾄﾝ_圧力現在値_kpa_"].cast(float), None
        ).max(),
        立上_ピストン現在_min=cond_start.ifelse(
            t_featured["成形ﾋﾟｽﾄﾝ_圧力現在値_kpa_"].cast(float), None
        ).min(),
        立上_ピストン現在_std=cond_start.ifelse(
            t_featured["成形ﾋﾟｽﾄﾝ_圧力現在値_kpa_"].cast(float), None
        ).std(),
        立上_ピストン目標_max=cond_start.ifelse(
            t_featured["成形ﾋﾟｽﾄﾝ_圧力目標値_kpa_"].cast(float), None
        ).max(),
        立上_ピストン目標_min=cond_start.ifelse(
            t_featured["成形ﾋﾟｽﾄﾝ_圧力目標値_kpa_"].cast(float), None
        ).min(),
        立上_ピストン目標_std=cond_start.ifelse(
            t_featured["成形ﾋﾟｽﾄﾝ_圧力目標値_kpa_"].cast(float), None
        ).std(),
        立上_テーブル差分_max=cond_start.ifelse(
            t_featured["テーブル位置差分_um"].cast(float), None
        ).max(),
        立上_テーブル差分_min=cond_start.ifelse(
            t_featured["テーブル位置差分_um"].cast(float), None
        ).min(),
        立上_テーブル差分_std=cond_start.ifelse(
            t_featured["テーブル位置差分_um"].cast(float), None
        ).std(),
        立上_テーブル現在_max=cond_start.ifelse(
            t_featured["成形ﾃｰﾌﾞﾙ1_現在位置_um_"].cast(float), None
        ).max(),
        立上_テーブル現在_min=cond_start.ifelse(
            t_featured["成形ﾃｰﾌﾞﾙ1_現在位置_um_"].cast(float), None
        ).min(),
        立上_テーブル現在_std=cond_start.ifelse(
            t_featured["成形ﾃｰﾌﾞﾙ1_現在位置_um_"].cast(float), None
        ).std(),
        立上_テーブル目標_max=cond_start.ifelse(
            t_featured["成形ﾃｰﾌﾞﾙ1_指令位置_um_"].cast(float), None
        ).max(),
        立上_テーブル目標_min=cond_start.ifelse(
            t_featured["成形ﾃｰﾌﾞﾙ1_指令位置_um_"].cast(float), None
        ).min(),
        立上_テーブル目標_std=cond_start.ifelse(
            t_featured["成形ﾃｰﾌﾞﾙ1_指令位置_um_"].cast(float), None
        ).std(),
        立上_疑似流路抵抗_max=cond_start.ifelse(
            t_featured["疑似流路抵抗"].cast(float), None
        ).max(),
        立上_疑似流路抵抗_mean=cond_start.ifelse(
            t_featured["疑似流路抵抗"].cast(float), None
        ).mean(),
        立上_疑似流路抵抗_std=cond_start.ifelse(
            t_featured["疑似流路抵抗"].cast(float), None
        ).std(),
        立上_速度_avg=cond_start.ifelse(
            t_featured["成形ﾋﾟｽﾄﾝ_現在速度_um_s_"].cast(float), None
        ).mean(),
        立上_速度_max=cond_start.ifelse(
            t_featured["成形ﾋﾟｽﾄﾝ_現在速度_um_s_"].cast(float), None
        ).max(),
        立上_ﾍｯﾄﾞ1ﾄﾙｸ_avg=cond_start.ifelse(
            t_featured["成形ﾍｯﾄﾞ1_帰還ﾄﾙｸ_0_01per_"].cast(float), None
        ).mean(),
        立上_ﾍｯﾄﾞ2ﾄﾙｸ_avg=cond_start.ifelse(
            t_featured["成形ﾍｯﾄﾞ2_帰還ﾄﾙｸ_0_01per_"].cast(float), None
        ).mean(),
        # ------------------------------------------
        # 【2. 定常フェーズ (Steady)】 ★最重要区間
        # ------------------------------------------
        定常_ﾄﾙｸ_avg=cond_steady.ifelse(
            t_featured["成形ﾋﾟｽﾄﾝ_帰還ﾄﾙｸ_0_01per_"].cast(float), None
        ).mean(),
        定常_ﾄﾙｸ_max=cond_steady.ifelse(
            t_featured["成形ﾋﾟｽﾄﾝ_帰還ﾄﾙｸ_0_01per_"].cast(float), None
        ).max(),
        定常_ﾄﾙｸ_min=cond_steady.ifelse(
            t_featured["成形ﾋﾟｽﾄﾝ_帰還ﾄﾙｸ_0_01per_"].cast(float), None
        ).min(),
        定常_ﾄﾙｸ_std=cond_steady.ifelse(
            t_featured["成形ﾋﾟｽﾄﾝ_帰還ﾄﾙｸ_0_01per_"].cast(float), None
        ).std(),
        定常_ピストン差分_max=cond_steady.ifelse(
            t_featured["ピストン圧力差分_kpa"].cast(float), None
        ).max(),
        定常_ピストン差分_min=cond_steady.ifelse(
            t_featured["ピストン圧力差分_kpa"].cast(float), None
        ).min(),
        定常_ピストン差分_std=cond_steady.ifelse(
            t_featured["ピストン圧力差分_kpa"].cast(float), None
        ).std(),
        定常_ピストン現在_max=cond_steady.ifelse(
            t_featured["成形ﾋﾟｽﾄﾝ_圧力現在値_kpa_"].cast(float), None
        ).max(),
        定常_ピストン現在_min=cond_steady.ifelse(
            t_featured["成形ﾋﾟｽﾄﾝ_圧力現在値_kpa_"].cast(float), None
        ).min(),
        定常_ピストン現在_std=cond_steady.ifelse(
            t_featured["成形ﾋﾟｽﾄﾝ_圧力現在値_kpa_"].cast(float), None
        ).std(),
        定常_ピストン目標_max=cond_steady.ifelse(
            t_featured["成形ﾋﾟｽﾄﾝ_圧力目標値_kpa_"].cast(float), None
        ).max(),
        定常_ピストン目標_min=cond_steady.ifelse(
            t_featured["成形ﾋﾟｽﾄﾝ_圧力目標値_kpa_"].cast(float), None
        ).min(),
        定常_ピストン目標_std=cond_steady.ifelse(
            t_featured["成形ﾋﾟｽﾄﾝ_圧力目標値_kpa_"].cast(float), None
        ).std(),
        定常_テーブル差分_max=cond_steady.ifelse(
            t_featured["テーブル位置差分_um"].cast(float), None
        ).max(),
        定常_テーブル差分_min=cond_steady.ifelse(
            t_featured["テーブル位置差分_um"].cast(float), None
        ).min(),
        定常_テーブル差分_std=cond_steady.ifelse(
            t_featured["テーブル位置差分_um"].cast(float), None
        ).std(),
        定常_テーブル現在_max=cond_steady.ifelse(
            t_featured["成形ﾃｰﾌﾞﾙ1_現在位置_um_"].cast(float), None
        ).max(),
        定常_テーブル現在_min=cond_steady.ifelse(
            t_featured["成形ﾃｰﾌﾞﾙ1_現在位置_um_"].cast(float), None
        ).min(),
        定常_テーブル現在_std=cond_steady.ifelse(
            t_featured["成形ﾃｰﾌﾞﾙ1_現在位置_um_"].cast(float), None
        ).std(),
        定常_テーブル目標_max=cond_steady.ifelse(
            t_featured["成形ﾃｰﾌﾞﾙ1_指令位置_um_"].cast(float), None
        ).max(),
        定常_テーブル目標_min=cond_steady.ifelse(
            t_featured["成形ﾃｰﾌﾞﾙ1_指令位置_um_"].cast(float), None
        ).min(),
        定常_テーブル目標_std=cond_steady.ifelse(
            t_featured["成形ﾃｰﾌﾞﾙ1_指令位置_um_"].cast(float), None
        ).std(),
        定常_疑似流路抵抗_max=cond_steady.ifelse(
            t_featured["疑似流路抵抗"].cast(float), None
        ).max(),
        定常_疑似流路抵抗_mean=cond_steady.ifelse(
            t_featured["疑似流路抵抗"].cast(float), None
        ).mean(),
        定常_疑似流路抵抗_std=cond_steady.ifelse(
            t_featured["疑似流路抵抗"].cast(float), None
        ).std(),
        定常_速度_avg=cond_steady.ifelse(
            t_featured["成形ﾋﾟｽﾄﾝ_現在速度_um_s_"].cast(float), None
        ).mean(),
        定常_速度_max=cond_steady.ifelse(
            t_featured["成形ﾋﾟｽﾄﾝ_現在速度_um_s_"].cast(float), None
        ).max(),
        定常_ﾍｯﾄﾞ1ﾄﾙｸ_avg=cond_steady.ifelse(
            t_featured["成形ﾍｯﾄﾞ1_帰還ﾄﾙｸ_0_01per_"].cast(float), None
        ).mean(),
        定常_ﾍｯﾄﾞ2ﾄﾙｸ_avg=cond_steady.ifelse(
            t_featured["成形ﾍｯﾄﾞ2_帰還ﾄﾙｸ_0_01per_"].cast(float), None
        ).mean(),
        # ------------------------------------------
        # 【3. 立下フェーズ (End)】
        # ------------------------------------------
        立下_ﾄﾙｸ_avg=cond_end.ifelse(
            t_featured["成形ﾋﾟｽﾄﾝ_帰還ﾄﾙｸ_0_01per_"].cast(float), None
        ).mean(),
        立下_ﾄﾙｸ_max=cond_end.ifelse(
            t_featured["成形ﾋﾟｽﾄﾝ_帰還ﾄﾙｸ_0_01per_"].cast(float), None
        ).max(),
        立下_ﾄﾙｸ_min=cond_end.ifelse(
            t_featured["成形ﾋﾟｽﾄﾝ_帰還ﾄﾙｸ_0_01per_"].cast(float), None
        ).min(),
        立下_ﾄﾙｸ_std=cond_end.ifelse(
            t_featured["成形ﾋﾟｽﾄﾝ_帰還ﾄﾙｸ_0_01per_"].cast(float), None
        ).std(),
        立下_ピストン差分_max=cond_end.ifelse(
            t_featured["ピストン圧力差分_kpa"].cast(float), None
        ).max(),
        立下_ピストン差分_min=cond_end.ifelse(
            t_featured["ピストン圧力差分_kpa"].cast(float), None
        ).min(),
        立下_ピストン差分_std=cond_end.ifelse(
            t_featured["ピストン圧力差分_kpa"].cast(float), None
        ).std(),
        立下_ピストン現在_max=cond_end.ifelse(
            t_featured["成形ﾋﾟｽﾄﾝ_圧力現在値_kpa_"].cast(float), None
        ).max(),
        立下_ピストン現在_min=cond_end.ifelse(
            t_featured["成形ﾋﾟｽﾄﾝ_圧力現在値_kpa_"].cast(float), None
        ).min(),
        立下_ピストン現在_std=cond_end.ifelse(
            t_featured["成形ﾋﾟｽﾄﾝ_圧力現在値_kpa_"].cast(float), None
        ).std(),
        立下_ピストン目標_max=cond_end.ifelse(
            t_featured["成形ﾋﾟｽﾄﾝ_圧力目標値_kpa_"].cast(float), None
        ).max(),
        立下_ピストン目標_min=cond_end.ifelse(
            t_featured["成形ﾋﾟｽﾄﾝ_圧力目標値_kpa_"].cast(float), None
        ).min(),
        立下_ピストン目標_std=cond_end.ifelse(
            t_featured["成形ﾋﾟｽﾄﾝ_圧力目標値_kpa_"].cast(float), None
        ).std(),
        立下_テーブル差分_max=cond_end.ifelse(
            t_featured["テーブル位置差分_um"].cast(float), None
        ).max(),
        立下_テーブル差分_min=cond_end.ifelse(
            t_featured["テーブル位置差分_um"].cast(float), None
        ).min(),
        立下_テーブル差分_std=cond_end.ifelse(
            t_featured["テーブル位置差分_um"].cast(float), None
        ).std(),
        立下_テーブル現在_max=cond_end.ifelse(
            t_featured["成形ﾃｰﾌﾞﾙ1_現在位置_um_"].cast(float), None
        ).max(),
        立下_テーブル現在_min=cond_end.ifelse(
            t_featured["成形ﾃｰﾌﾞﾙ1_現在位置_um_"].cast(float), None
        ).min(),
        立下_テーブル現在_std=cond_end.ifelse(
            t_featured["成形ﾃｰﾌﾞﾙ1_現在位置_um_"].cast(float), None
        ).std(),
        立下_テーブル目標_max=cond_end.ifelse(
            t_featured["成形ﾃｰﾌﾞﾙ1_指令位置_um_"].cast(float), None
        ).max(),
        立下_テーブル目標_min=cond_end.ifelse(
            t_featured["成形ﾃｰﾌﾞﾙ1_指令位置_um_"].cast(float), None
        ).min(),
        立下_テーブル目標_std=cond_end.ifelse(
            t_featured["成形ﾃｰﾌﾞﾙ1_指令位置_um_"].cast(float), None
        ).std(),
        立下_疑似流路抵抗_max=cond_end.ifelse(
            t_featured["疑似流路抵抗"].cast(float), None
        ).max(),
        立下_疑似流路抵抗_mean=cond_end.ifelse(
            t_featured["疑似流路抵抗"].cast(float), None
        ).mean(),
        立下_疑似流路抵抗_std=cond_end.ifelse(
            t_featured["疑似流路抵抗"].cast(float), None
        ).std(),
        立下_速度_avg=cond_end.ifelse(
            t_featured["成形ﾋﾟｽﾄﾝ_現在速度_um_s_"].cast(float), None
        ).mean(),
        立下_速度_max=cond_end.ifelse(
            t_featured["成形ﾋﾟｽﾄﾝ_現在速度_um_s_"].cast(float), None
        ).max(),
        立下_ﾍｯﾄﾞ1ﾄﾙｸ_avg=cond_end.ifelse(
            t_featured["成形ﾍｯﾄﾞ1_帰還ﾄﾙｸ_0_01per_"].cast(float), None
        ).mean(),
        立下_ﾍｯﾄﾞ2ﾄﾙｸ_avg=cond_end.ifelse(
            t_featured["成形ﾍｯﾄﾞ2_帰還ﾄﾙｸ_0_01per_"].cast(float), None
        ).mean(),
    )
    print(f"=================={C_A}極_スラリ重量読み込み============================")

    table_union = None

    for goki in range(
        int(goki_extract), int(goki_extract) + 1, 1
    ):  # tables は ibis テーブルのリスト
        table_juryo = f"uc{goki}_{C_A}極巻き出しplc_log3"
        table_org = con_eq.table(table_juryo)
        table_org = table_org.mutate(goki=ibis.literal(goki))

        if table_union is None:
            table_union = table_org
        else:
            table_union = table_union.union(table_org)

    table_juryo = table_union
    table_juryo = table_juryo.filter(
        (table_juryo["date_time"] >= ibis.literal(start_time))
        & (table_juryo["date_time"] <= ibis.literal(end_time))
    )

    columns = [f"{C_A}極箔id", "成形後重量測定結果", "差分重量_ｽﾗﾘｰ重量_", "goki"]
    table_juryo = table_juryo[columns]

    print(f"=================={C_A}極_スラリ厚み読み込み============================")
    table_union = None

    for goki in range(
        int(goki_extract), int(goki_extract) + 1, 1
    ):  # tables は ibis テーブルのリスト
        TABLE_ATUMI = f"uc{goki}_{C_A}極巻き出しplc_log4"
        table_org = con_eq.table(TABLE_ATUMI)
        if table_union is None:
            table_union = table_org
        else:
            table_union = table_union.union(table_org)

    table_atumi = table_union
    table_atumi = tu.add_row_mean(
        table_atumi,
        thickness_columns,
        new_col="全体厚み平均",
    )
    table_atumi = tu.add_row_stddev(
        table_atumi,
        thickness_columns,
        new_col="厚みstd",
    )

    joined_ab = table_juryo.join(
        table_atumi,
        predicates=[
            table_juryo[f"{C_A}極箔id"] == table_atumi[f"{C_A}極箔id"],
        ],
    )
    joined_ab = joined_ab.filter(
        (joined_ab["date_time"] >= ibis.literal(start_time))
        & (joined_ab["date_time"] <= ibis.literal(end_time))
    ).select(
        [
            "正極箔id",
            "全体厚み平均",
            "厚みstd",
            "差分重量_ｽﾗﾘｰ重量_",
            "厚み判定結果",
            "成形後重量測定結果",
        ]
    )
    df_main = (
        joined_ab.join(
            t_agg,
            predicates=[
                joined_ab[f"{C_A}極箔id"] == t_agg["ﾜｰｸid情報"],
            ],
        )
        .join(
            table_caid,
            predicates=[
                t_agg["ﾜｰｸid情報"] == table_caid["箔id"],
            ],
        )
        .execute()
    )
    df_main = df_main.round(4)
    df_main.to_csv(
        base_dir / "data" / f"goki{goki_extract}_{start_time}_{end_time}.csv",
        index=False,
    )

# %%
