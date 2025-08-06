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

    def _get_result_metadata(self, db: DbConnection, sql: str) -> list[str]:
        """Get column names from query result metadata"""
        try:
            cursor = db.execute(sql)
            return [col[0] for col in cursor.description] if cursor.description else []
        except Exception:
            return []

    def _has_column_mismatch(self, question: str, columns: list[str]) -> bool:
        """Check if returned columns match question intent using NLP patterns and semantic similarity"""
        if not columns:
            return False
            
        # Extract key terms from question
        question_lower = question.lower()
        expected_terms = set()
        
        # Look for "show me X" patterns
        if "show me" in question_lower:
            expected_terms.update(re.findall(r'show me ([\w\s]+?)(?:$|,|\.|;)', question_lower))
            
        # Look for "what is X" patterns  
        expected_terms.update(re.findall(r'what (?:is|are) (?:the )?([\w\s]+?)(?:$|,|\.|;)', question_lower))
        
        # Look for direct column mentions
        expected_terms.update(re.findall(r'(?:column|field)s? ([\w\s]+?)(?:$|,|\.|;)', question_lower))
        
        # Check if any expected term appears in result columns
        columns_lower = [col.lower() for col in columns]
        for term in expected_terms:
            term = term.strip()
            if not term:
                continue
                
            # Basic string matching
            if any(term in col or col in term for col in columns_lower):
                continue
                
            # Split into words and check individual components
            term_words = term.split()
            col_words = [w for col in columns_lower for w in col.split('_')]
            
            # Check if any word from term appears in any column
            if not any(any(word in col_word for col_word in col_words) for word in term_words):
                return True
                
        return False

    def _should_regenerate(self, error: str, sql: str) -> bool:
        """Determine if regeneration should be attempted based on error type"""
        error_lower = error.lower()
        
        # High confidence errors that warrant regeneration
        if any(e in error_lower for e in [
            "no such column",
            "no such table",
            "syntax error near",
            "unexpected token",
            "mismatched input"
        ]):
            return True
            
        # Medium confidence errors - only if basic query structure exists
        if any(e in error_lower for e in [
            "syntax error",
            "missing",
            "invalid"
        ]) and ("select" in sql.lower() and "from" in sql.lower()):
            return True
            
        return False

    def predict_sql(self, context: ContextData, db: DbConnection | None = None) -> str:
        if self.hint_filter_user_prompt and self.hint_filter_user_prompt:
            context.hints = self._filter_hints(context)
        messages = [
            SystemMessage(content=self._build_system_prompt(context)),
            HumanMessage(content=self._build_user_prompt(context)),
        ]

        result = self.model.invoke(messages)
        sql = self._parse_sql(result)

        logging.debug(
            f"Результат первой генерации: {result} for question: {context.question}"
        )
        
        # First try execution and column check
        cur_try = 0
        while cur_try < self.retries_num:
            try:
                db.execute(sql)
                columns = self._get_result_metadata(db, sql)
                if self._has_column_mismatch(context.question, columns):
                    raise ValueError("Possible column mismatch in results")
                return sql
            except Exception as e:
                error_str = str(e)
                if not self._should_regenerate(error_str, sql):
                    return sql
                    
                print(
                    f'Generated SQL executed with error: {error_str}. Regenerating sql for question: "{context.question}"'
                )
                messages = [
                    SystemMessage(
                        content=self._build_regen_system_prompt(context, sql, error_str)
                    ),
                    HumanMessage(
                        content=self._build_regen_user_prompt(context, sql, error_str)
                    ),
                ]
                result = self.model.invoke(messages)
                sql = self._parse_sql(result)
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
    def _parse_sql(response: BaseMessage) -> str:
        sql = response.content
        if "```sql" in sql:
            sql = sql.split("```sql")[1].strip().split("```")[0]
        sql = (
            sql.replace("\r\n", " ")
            .replace("\\n", " ")
            .replace("\n", " ")
            .strip()
            .replace(" +", " ")
            .strip()
        )
        if "<think>" in sql:
            sql = re.sub(r"<think>.*?</think>\n?", "", sql, flags=re.DOTALL)
        return sql

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

        return system_prompt

    def _build_regen_user_prompt(
        self, context: ContextData, failed_sql: str, sql_error: str
    ) -> str:
        # Get column metadata if available
        columns = []
        try:
            columns = self._get_result_metadata(context.db, failed_sql)
        except Exception:
            pass
            
        # Analyze error type for more specific feedback
        error_feedback = ""
        error_lower = sql_error.lower()
        
        # Only trigger regeneration for clear, actionable errors
        if ("syntax error" in error_lower and 
            any(e in error_lower for e in ["near", "unexpected", "mismatched"])):
            error_feedback = "\n\nThere appears to be a SQL syntax error near:"
            # Extract the problematic part from error message
            if "near" in error_lower:
                near_part = error_lower.split("near")[-1].split("\n")[0].strip(" '\"")
                error_feedback += f" '{near_part}'"
            error_feedback += "\nPlease check:"
            error_feedback += "\n- Balanced parentheses and quotes"
            error_feedback += "\n- Proper JOIN conditions"
            
        elif "no such column" in error_lower:
            # Extract column name from error if possible
            col_name = re.search(r"no such column: (\w+)", error_lower)
            if col_name:
                error_feedback = f"\n\nThe column '{col_name.group(1)}' doesn't exist. Check:"
                error_feedback += "\n- Spelling and table prefixes"
                error_feedback += "\n- Schema for available columns"
            else:
                error_feedback = "\n\nInvalid column reference. Verify all column names exist."
                
        elif "no such table" in error_lower:
            # Extract table name from error if possible
            table_name = re.search(r"no such table: (\w+)", error_lower)
            if table_name:
                error_feedback = f"\n\nThe table '{table_name.group(1)}' doesn't exist. Check:"
                error_feedback += "\n- Spelling and schema definition"
            else:
                error_feedback = "\n\nInvalid table reference. Verify all table names exist."
            
        column_feedback = ""
        if columns:
            column_feedback = (
                f"\n\nThe query returned these columns: {', '.join(columns)}. "
                "Make sure these match what was asked for in the question."
            )

        system_prompt = self.regen_user_prompt.format(
            question=context.question,
            sql=failed_sql,
            result=sql_error + column_feedback,
            gold=self._gold_to_str(context.gold_recs) if context.gold_recs else "",
            hints=self._hints_to_str(context.hints) if context.hints else "",
            ddl=self._ddl_to_str(context.ddl) if context.ddl else "",
            stats=self._tables_info_to_str(context.tables_info)
            if context.tables_info
            else "",
        )

        return system_prompt


PROMPT_DATA = {
    "system_prompt": "Придумайте стратегию последовательного выполнения шагов для решения задачи. Изучите доступные варианты и выберите самый эффективный. Интегрируйте важные элементы SELECT, FROM и WHERE в ваш SQL-запрос. Проверьте правильность кода с использованием платформы DB-Fiddle и исправьте выявленные недочеты. {hints} \n\n {ddl} {gold} {stats} ",
    "user_prompt": "Убедись, что колонки из запроса существуют в нужных таблицах.Используй ТОЛЬКО продемонстрированные в схеме и примерах колонки. Если колонки нет в схеме или базе данных не пытайся ее подставить. В ответе укажи только SQL-запрос, больше ничего.Напиши SQL запрос, чтобы ответить на следующий вопрос:  {question}. \\n ",
    "regen_system_prompt": "Ты профессиональный аналитик баз данных Postgres. Тебе необходимо исправить SQL-запрос, чтобы он выдавал правильные данные. Результат выполнения запроса: {result} . {hints} \n\n {ddl} {stats} ",
    "regen_user_prompt": "Тебе необходимо исправить SQL-запрос. Вопрос пользователя: {question} . SQL: ```sql\n {sql} ```. \nПроверь особенно тщательно колонки в SELECT - они должны соответствовать вопросу. Убедись, что колонки из запроса существуют в нужных таблицах. Используй ТОЛЬКО продемонстрированные в схеме и примерах колонки. Если колонки нет в схеме или базе данных не пытайся ее подставить. В ответе укажи только SQL-запрос, больше ничего. Исправь SQL-запрос. \\n {question} "
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
                "model": "deepseek-coder",
                "base_url": "https://api.deepseek.com",
                "credentials": os.getenv("DEEPSEEK_API_KEY"),
                "verify_ssl_certs": False,
                "temperature": 0,
                "timeout": 60000,
                "model_name": "deepseek",
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


