import argparse
import logging
import re
import os
import json
import numpy as np

from langchain.schema import HumanMessage, SystemMessage
from langchain_core.messages import BaseMessage
from langchain_gigachat import GigaChat
import src.text2sql_bench.settings  # noqa
from src.text2sql_bench.core.benchmark import BenchRunner
from src.text2sql_bench.core.model import BenchConfig, RunConfig
from src.text2sql_bench.dataset import load_datasets
from src.text2sql_bench.db import DbConnection
from src.text2sql_bench.core.model import ContextData
from src.text2sql_bench.db import DbConnection
from src.text2sql_bench.model_wrappers.wrapper import ModelWrapper
from src.text2sql_bench.vector_db import GoldRecord, TableInfo


class DeepseekAIScientist(ModelWrapper):
    def __init__(
        self,
        model: str,
        base_url: str,
        verify_ssl_certs: bool,
        temperature: float,
        timeout: int,
        prompt_data: dict[str, str],
        profanity_check: bool = False,
        user: str | None = None,
        password: str | None = None,
        cert_file: str | None = None,
        key_file: str | None = None,
        credentials: str | None = None,
        model_name: str | None = "deepseek-coder-v2",
        retries_num: int = 3,
        schema_type: str | None = None,
        **model_kwargs,
    ):
        giga = GigaChat(
            model=model,
            base_url=base_url,
            cert_file=cert_file,
            key_file=key_file,
            user=user,
            password=password,
            credentials=credentials,
            access_token=credentials,
            verify_ssl_certs=verify_ssl_certs,
            profanity_check=profanity_check,
            temperature=temperature,
            timeout=timeout,
            **model_kwargs,
        )
        self._init_prompt(prompt_data)
        self.model = giga
        self.model_name = model_name

        self.retries_num = retries_num
        self.schema_type = schema_type

    def predict_sql(self, context: ContextData, db: DbConnection | None = None) -> str:
        if self.hint_filter_user_prompt and self.hint_filter_user_prompt:
            context.hints = self._filter_hints(context)
        # Generate initial SQL and reasoning
        messages = [
            SystemMessage(content=self._build_system_prompt(context)),
            HumanMessage(content=self._build_user_prompt(context)),
        ]
        result = self.model.invoke(messages)
        reasoning, sql = self._parse_sql(result)
        
        # Verify reasoning against schema
        valid_reasoning = self._verify_reasoning(reasoning, context)
        
        if not valid_reasoning:
            # Attempt single regeneration focused on fixing invalid reasoning
            messages = [
                SystemMessage(content=self._build_schema_error_prompt(context, reasoning)),
                HumanMessage(content=self._build_user_prompt(context)),
            ]
            result = self.model.invoke(messages)
            reasoning, sql = self._parse_sql(result)
        
        # Execute SQL and handle potential errors
        cur_try = 0
        while cur_try < self.retries_num:
            try:
                db.execute(sql)
                return sql
            except Exception as e:
                print(
                    f'Generated SQL executed with error: {e}. Regenerating sql for question: "{context.question}"'
                )
                messages = [
                    SystemMessage(
                        content=self._build_regen_system_prompt(context, sql, str(e))
                    ),
                    HumanMessage(
                        content=self._build_regen_user_prompt(context, sql, str(e))
                    ),
                ]
                result = self.model.invoke(messages)
                reasoning, sql = self._parse_sql(result)
                cur_try += 1

                logging.debug(
                    f"Результат после перегенерации {cur_try}: {result} for question: {context.question}"
                )

        return sql

    @staticmethod
    def _to_chunks(arr: list[any], size: int) -> list[list[any]]:
        return [arr[i : i + size] for i in range(0, len(arr), size)]

    def _filter_hints(self, context: ContextData) -> list[str]:
        if not self.hint_filter_system_prompt:
            raise ValueError("Missing 'hint_filter_system_prompt' key in the prompt")
        if not self.hint_filter_user_prompt:
            raise ValueError("Missing 'hint_filter_user_prompt' key in the prompt")
        if context.hints:
            hints_str = "\n".join(context.hints)
        else:
            hints_str = ""
        logging.debug(f"Question: {context.question}")
        logging.debug(f"Original hints: \n{hints_str}")
        chunk_size = 5
        result = []
        if context.hints:
            hint_chunks = self._to_chunks(context.hints, chunk_size)
        else:
            hint_chunks = []
        for chunk in hint_chunks:
            messages = [
                SystemMessage(
                    content=self.hint_filter_system_prompt.format(
                        hints="\n".join(chunk),
                        ddl=self._ddl_to_str(context.ddl) if context.ddl else "",
                        gold=self._gold_to_str(context.gold_recs)
                        if context.gold_recs
                        else "",
                        stats=self._tables_info_to_str(context.tables_info)
                        if context.tables_info
                        else "",
                        question=context.question,
                    )
                ),
                HumanMessage(
                    content=self.hint_filter_user_prompt.format(
                        hints="\n".join(chunk),
                        ddl=self._ddl_to_str(context.ddl) if context.ddl else "",
                        gold=self._gold_to_str(context.gold_recs)
                        if context.gold_recs
                        else "",
                        stats=self._tables_info_to_str(context.tables_info)
                        if context.tables_info
                        else "",
                        question=context.question,
                    )
                ),
            ]
            response = self.model.invoke(messages).content
            filtered_hints = [] if "NONE" == response else response.split("\n")
            result.extend(filtered_hints)
            hints_str = "\n".join(result)
        logging.debug(f"Reduced hints: \n {hints_str}")
        return result

    def enhance_question(self, question: str):
        if self.enhance_system_prompt:
            messages = [
                SystemMessage(content=self.enhance_system_prompt),
                HumanMessage(content=question),
            ]
            return self.model.invoke(messages).content
        else:
            raise ValueError("Missing 'enhance_system_prompt' key in the prompt")

    def name(self) -> str:
        return self.model_name if self.model_name is not None else self.model.model

    @staticmethod
    def _parse_sql(response: BaseMessage) -> tuple[str, str]:
        content = response.content
        
        # Extract reasoning inside <think></think> tags
        reasoning_match = re.search(r'<think>(.*?)</think>', content, re.DOTALL)
        reasoning = reasoning_match.group(1).strip() if reasoning_match else ''
        
        # Extract SQL between ```sql``` tags
        sql_match = re.search(r'```sql\s*(.*?)```', content, re.DOTALL)
        sql = sql_match.group(1).strip() if sql_match else ''
        
        # Clean up SQL formatting
        sql = (
            sql.replace("\r\n", " ")
            .replace("\\n", " ")
            .replace("\n", " ")
            .strip()
            .replace(" +", " ")
            .strip()
        )
        
        return reasoning, sql

    def _init_prompt(self, prompt_data: dict[str, str]):
        if "system_prompt" not in prompt_data:
            raise ValueError("Missing 'system_prompt' key in the prompt")
        if "user_prompt" not in prompt_data:
            raise ValueError("Missing 'user_prompt' key in the prompt")
        if "regen_system_prompt" not in prompt_data:
            raise ValueError("Missing 'regen_system_prompt' key in the prompt")
        if "regen_user_prompt" not in prompt_data:
            raise ValueError("Missing 'regen_user_prompt' key in the prompt")

        self.system_prompt = prompt_data["system_prompt"]
        self.user_prompt = prompt_data["user_prompt"]
        self.regen_system_prompt = prompt_data["regen_system_prompt"]
        self.regen_user_prompt = prompt_data["regen_user_prompt"]

        self.hint_filter_system_prompt = prompt_data.get(
            "hint_filter_system_prompt", ""
        )
        self.hint_filter_user_prompt = prompt_data.get("hint_filter_user_prompt", "")
        self.enhance_system_prompt = prompt_data.get("enhance_system_prompt", "")

    @staticmethod
    def _gold_to_str(gold: list[GoldRecord]) -> str:
        text_gold_recs: list[str] = []
        for g in gold:
            text_gold_recs.append(f"Вопрос:{g.question}: ```sql\n{g.sql}\n```")
        gold_str = "\n".join(text_gold_recs)
        return f"""\nПримеры sql запросов: {gold_str}"""

    @staticmethod
    def _hints_to_str(hints: list[str]) -> str:
        hints_str = "\n".join(hints)
        return f"""Вот полезная информация которую нужно использовать в SELECT: \n{hints_str}"""

    @staticmethod
    def _ddl_to_str(ddl: str) -> str:
        if ddl:
            return f"""Схема базы: {ddl}"""
        else:
            return ""

    def _tables_info_to_str(self, tables_info: list[TableInfo]) -> str:
        if not self.schema_type:
            text_cols_info: list[str] = []
            for table_info in tables_info:
                table_name = table_info.name
                for col_info in table_info.cols_info:
                    text_cols_info.append(
                        f"Таблица: {table_name}, {col_info.pretty_print()}"
                    )
            cols_str = "\n".join(text_cols_info)
            return f"""\nДополнительная информация: {cols_str}"""
        elif self.schema_type == "M-schema":
            text_cols_info: list[str] = ["【Schema】"]
            for table_info in tables_info:
                table_name = table_info.name
                text_cols_info.extend([f"# Table: {table_name}", "["])
                for i, col_info in enumerate(table_info.cols_info):
                    col_str = (
                        f"({col_info.name}:{col_info.data_type},{col_info.description},"
                        f"Examples: {col_info.categories if col_info.categories else col_info.samples})"
                    )
                    if i < len(table_info.cols_info) - 1:
                        col_str += ","
                    text_cols_info.append(col_str)
                text_cols_info.append("]")
            final_str = "\n".join(text_cols_info)
            return final_str
        else:
            raise NotImplementedError

    def _build_system_prompt(self, context: ContextData) -> str:
        system_prompt = self.system_prompt.format(
            hints=self._hints_to_str(context.hints) if context.hints else "",
            ddl=self._ddl_to_str(context.ddl) if context.ddl else "",
            gold=self._gold_to_str(context.gold_recs) if context.gold_recs else "",
            stats=self._tables_info_to_str(context.tables_info)
            if context.tables_info
            else "",
        )

        return system_prompt

    def _build_user_prompt(self, context: ContextData) -> str:
        user_prompt = self.user_prompt.format(
            hints=self._hints_to_str(context.hints) if context.hints else "",
            ddl=self._ddl_to_str(context.ddl) if context.ddl else "",
            gold=self._gold_to_str(context.gold_recs) if context.gold_recs else "",
            stats=self._tables_info_to_str(context.tables_info)
            if context.tables_info
            else "",
            question=context.question,
        )

        return user_prompt

    def _build_regen_system_prompt(
        self, context: ContextData, failed_sql: str, sql_error: str
    ) -> str:
        # Determine if the failure stems from schema-related issues
        reasoning = self._extract_reasoning_from_sql(failed_sql)
        if self._verify_reasoning(reasoning, context):
            # Regular regeneration flow
            system_prompt = self.regen_system_prompt.format(
                sql=failed_sql,
                result=sql_error,
                gold=self._gold_to_str(context.gold_recs) if context.gold_recs else "",
                hints=self._hints_to_str(context.hints) if context.hints else "",
                ddl=self._ddl_to_str(context.ddl) if context.ddl else "",
                stats=self._tables_info_to_str(context.tables_info)
                if context.tables_info
                else "",
            )
        else:
            # Schema-specific error handling
            system_prompt = self._build_schema_error_prompt(context, reasoning)

        return system_prompt

    def _verify_reasoning(self, reasoning: str, context: ContextData) -> bool:
        # Parse reasoning to extract tables, columns, and join conditions
        # For simplicity, assume reasoning follows a structured pattern
        # Example reasoning structure:
        #   Tables: users, orders
        #   Columns: id, username, order_date
        #   Joins: users.id = orders.user_id
        
        # Split reasoning into sections
        parts = reasoning.strip().split('\n')
        tables_section = next((p for p in parts if p.startswith('Tables:')), '')
        columns_section = next((p for p in parts if p.startswith('Columns:')), '')
        joins_section = next((p for p in parts if p.startswith('Joins:')), '')
        
        # Extract tables, columns, and joins
        tables = set(re.findall(r'\b\w+\b', tables_section)) if tables_section else set()
        columns = set(re.findall(r'\b\w+\b', columns_section)) if columns_section else set()
        joins = re.findall(r'(\w+)\.(\w+) *= *(\w+)\.(\w+)', joins_section) if joins_section else []
        
        # Verify tables exist in DDL
        ddl_tables = set(re.findall(r'TABLE (\w+)', context.ddl)) if context.ddl else set()
        if not tables.issubset(ddl_tables):
            return False
            
        # Verify columns exist in DDL
        ddl_columns = set(re.findall(r'COLUMN (\w+)', context.ddl)) if context.ddl else set()
        if not columns.issubset(ddl_columns):
            return False
            
        # Verify joins follow FK constraints
        for left_table, left_col, right_table, right_col in joins:
            # Check if left_col exists in left_table and right_col exists in right_table
            pass  # TO BE IMPLEMENTED BASED ON ACTUAL SCHEMA STRUCTURE
        
        return True

    def _build_schema_error_prompt(self, context: ContextData, reasoning: str) -> str:
        # Construct a detailed error prompt based on schema mismatch
        # Highlight missing columns, incorrect joins, nonexistent tables
        pass  # TO BE IMPLEMENTED WITH SPECIFIC ERROR DETAILS


PROMPT_DATA = {
    "system_prompt": """Используя предоставленную информацию о структуре базы данных и примеры предыдущих успешных запросов, сформулируй логику рассуждений перед созданием SQL-запроса. Обрати внимание на следующие аспекты: 
    1. Какие таблицы и столбцы необходимы?
    2. Как связаны эти таблицы через внешние ключи?
    3. Какие условия фильтрации нужны? 
    
    После этого создай сам SQL-запрос. Используй тег <think> для обозначения своих размышлений перед самим запросом. Например:
    ```
    <think>Рассуждаем...</think>
    SELECT ...;
    ```
    
    {hints} \n\n {ddl} {gold} {stats} """,
    "user_prompt": """
    Сначала определи необходимые шаги для ответа на этот вопрос. Затем напиши SQL-запрос внутри тега <think>. Пример:
    ```
    <think>Шаг 1: Определим нужные таблицы и их связи.</think>
    SELECT ...
    ```
    
    Вопрос пользователя: {question}.
    """,
    "regen_system_prompt": """
    Ты профессиональный аналитик баз данных PostgreSQL. Необходимо исправить предыдущий SQL-запрос так, чтобы он корректно выполнялся и возвращал ожидаемые результаты. При этом важно следовать следующим правилам:
    1. Использовать только те таблицы и столбцы, которые указаны в схеме.
    2. Проверять наличие внешних ключей при использовании JOIN.
    3. Применять фильтры только по существующим столбцам.
    
    Ошибка предыдущего запроса: {result}. Пожалуйста, сначала объясни свои действия в теге <think>, а затем представь новый SQL-запрос.
    {hints} \n\n {ddl} {stats}
    """,
    "regen_user_prompt": """
    Исправь предыдущий SQL-запрос. Объясни свои изменения в теге <think>.
    Вопрос пользователя: {question}.
    Предыдущий SQL-запрос: ```sql\n {sql} ```
    Ошибка: {result}.
    """
}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run text2sql benchmark")
    parser.add_argument("--out_dir", type=str, default="run_0", help="Output directory")
    args = parser.parse_args()

    easy_medium, total, bucket_counts = BenchRunner(
        report_manager=None,
        bench_name="Test",
        dataset=load_datasets({"vacancies_normalized_duck"})["vacancies_normalized_duck"],
        output_path=args.out_dir
    ).run(
        model=DeepseekAIScientist(
            **{
                "model": "GigaChat-2-Max",
                "base_url": "https://beta.saluteai.sberdevices.ru/v1",
                "credentials": os.getenv("GIGACHAT_API_KEY"),
                "verify_ssl_certs": False,
                "temperature": 0,
                "timeout": 60000,
                "model_name": "GigaChat-2-Max",
                "retries_num": 3,
                "prompt_data": PROMPT_DATA
            }),
        prompt_name="Test",
        config=RunConfig(
            **{
                "dataset": "vacancies_normalized_duck",
                "use_stat": True,
                "use_gold": True,
                "save_report": False,
                "top_g": 10
            }
        )
    )

    final_info = {
        "bench":
            {
                "means":
                    {
                        "easy_medium": easy_medium,
                        "total": total,
                        "counts": bucket_counts
                    }
            }
    }

    with open(os.path.join(args.out_dir, "final_info.json"), "w") as f:
        json.dump(final_info, f, indent=4)

    with open(os.path.join(args.out_dir, "all_results.npy"), "wb") as f:
        np.save(f, final_info)


