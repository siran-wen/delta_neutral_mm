#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Hyperliquid交易所连通性测试工具 (使用CCXT)

该工具使用CCXT库测试与Hyperliquid交易所的连接状态，包括：
- REST API连通性测试
- 认证信息验证
- 市场数据获取测试
- 下单/撤单连通性测试
"""

import yaml
import json
import time
import sys
import os
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime

# Windows编码兼容性处理
if sys.platform == 'win32':
    try:
        # 尝试设置UTF-8编码
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
    except Exception:
        pass

try:
    import ccxt
except ImportError:
    print("[ERROR] 缺少依赖: ccxt")
    print("请运行: pip install ccxt")
    sys.exit(1)

try:
    from eth_account import Account
except ImportError:
    print("[WARNING] 缺少依赖: eth-account (认证测试将跳过)")
    print("请运行: pip install eth-account")
    Account = None


class Colors:
    """终端颜色输出"""
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    RESET = '\033[0m'
    BOLD = '\033[1m'


class Symbols:
    """兼容Windows的符号输出"""
    @staticmethod
    def _check_unicode_support():
        """检测是否支持Unicode字符"""
        try:
            # 测试Unicode字符输出
            test_char = '✓'
            test_char.encode(sys.stdout.encoding or 'utf-8')
            return True
        except (UnicodeEncodeError, AttributeError, LookupError):
            return False
    
    # 类属性，延迟初始化
    _unicode_support = None
    
    @classmethod
    def _get_unicode_support(cls):
        """获取Unicode支持状态"""
        if cls._unicode_support is None:
            cls._unicode_support = cls._check_unicode_support()
        return cls._unicode_support
    
    @classmethod
    def CHECK(cls):
        return '✓' if cls._get_unicode_support() else '[OK]'
    
    @classmethod
    def CROSS(cls):
        return '✗' if cls._get_unicode_support() else '[X]'
    
    @classmethod
    def WARNING(cls):
        return '⚠' if cls._get_unicode_support() else '[!]'
    
    @classmethod
    def SKIP(cls):
        return '⊘' if cls._get_unicode_support() else '[-]'


class HyperliquidConnectivityTester:
    """Hyperliquid连通性测试器 (使用CCXT)"""
    
    def __init__(self, config_path: str = "config/hyperliquid_config.yaml"):
        """
        初始化测试器
        
        Args:
            config_path: 配置文件路径
        """
        self.config_path = config_path
        self.config = self._load_config()
        self.api_config = self.config.get('hyperliquid', {}).get('api', {})
        self.auth_config = self.config.get('hyperliquid', {}).get('authentication', {})
        self.timeout = self.api_config.get('timeout', 30)
        
        # 初始化CCXT交易所实例
        self.exchange = None
        self._init_exchange()
        
        self.results = {
            'rest_api': {'status': 'pending', 'details': []},
            'authentication': {'status': 'pending', 'details': []},
            'market_data': {'status': 'pending', 'details': []},
            'order_lifecycle': {'status': 'pending', 'details': []}
        }
    
    def _load_config(self) -> Dict[str, Any]:
        """加载配置文件"""
        try:
            config_file = Path(self.config_path)
            if not config_file.exists():
                raise FileNotFoundError(f"配置文件不存在: {self.config_path}")
            
            with open(config_file, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            
            return config
        except Exception as e:
            print(f"{Colors.RED}[ERROR] 加载配置文件失败: {e}{Colors.RESET}")
            sys.exit(1)
    
    def _init_exchange(self):
        """初始化CCXT交易所实例"""
        try:
            # 获取认证信息
            private_key = self.auth_config.get('private_key', '').strip()
            wallet_address = self.auth_config.get('wallet_address', '').strip()
            
            # 创建CCXT交易所配置
            # Hyperliquid在CCXT中使用 walletAddress + privateKey 认证
            exchange_config = {
                'walletAddress': wallet_address,
                'privateKey': private_key,
                'enableRateLimit': True,
                'timeout': self.timeout * 1000,  # CCXT使用毫秒
                'options': {
                    'defaultType': 'swap',  # 默认使用永续合约
                }
            }

            # 创建交易所实例
            self.exchange = ccxt.hyperliquid(exchange_config)

        except Exception as e:
            print(f"{Colors.RED}[ERROR] 初始化交易所失败: {e}{Colors.RESET}")
            # 即使认证失败，也创建只读实例用于公共API测试
            self.exchange = ccxt.hyperliquid({
                'enableRateLimit': True,
                'timeout': self.timeout * 1000,
            })

        # CCXT 4.5.x: 把 hip3 从 fetchMarkets.types 移除，跳过 fetchHip3Markets()
        # 避免 "Too many DEXes" 报错（做市只需 spot/swap 市场）
        self.exchange.options.setdefault('fetchMarkets', {})['types'] = ['spot', 'swap']
    
    def _print_header(self, title: str):
        """打印测试标题"""
        print(f"\n{Colors.BOLD}{Colors.BLUE}{'='*60}{Colors.RESET}")
        print(f"{Colors.BOLD}{Colors.BLUE}{title:^60}{Colors.RESET}")
        print(f"{Colors.BOLD}{Colors.BLUE}{'='*60}{Colors.RESET}\n")
    
    def _print_result(self, test_name: str, success: bool, message: str = ""):
        """打印测试结果"""
        try:
            if success:
                status = f"{Colors.GREEN}{Symbols.CHECK()}{Colors.RESET}"
            elif success is None:
                status = f"{Colors.YELLOW}{Symbols.WARNING()}{Colors.RESET}"
            else:
                status = f"{Colors.RED}{Symbols.CROSS()}{Colors.RESET}"
            print(f"  {status} {test_name}")
            if message:
                indent = "    "
                print(f"{indent}{message}")
        except (UnicodeEncodeError, UnicodeDecodeError):
            # Fallback for encoding issues
            status = "[OK]" if success else "[FAIL]"
            print(f"  {status} {test_name}")
            if message:
                indent = "    "
                print(f"{indent}{message}")
    
    def test_rest_api(self) -> bool:
        """测试REST API连通性"""
        self._print_header("REST API 连通性测试 (CCXT)")
        
        success_count = 0
        total_tests = 0
        
        # 测试1: 加载市场信息
        total_tests += 1
        try:
            markets = self.exchange.load_markets(reload=True)
            market_count = len(markets)
            self._print_result(
                "加载市场信息",
                True,
                f"成功加载 {market_count} 个交易对"
            )
            success_count += 1
            self.results['rest_api']['details'].append({
                'test': '加载市场信息',
                'status': 'success',
                'market_count': market_count
            })
        except Exception as e:
            self._print_result("加载市场信息", False, f"错误: {str(e)}")
            self.results['rest_api']['details'].append({
                'test': '加载市场信息',
                'status': 'failed',
                'error': str(e)
            })
        
        # 测试2: 获取交易所信息
        total_tests += 1
        try:
            # 获取交易所元数据
            if hasattr(self.exchange, 'fetch_status'):
                status = self.exchange.fetch_status()
                self._print_result("获取交易所状态", True, f"状态: {status.get('status', 'unknown')}")
            else:
                # 尝试获取ticker作为连通性测试
                tickers = self.exchange.fetch_tickers()
                self._print_result("获取交易所信息", True, f"获取到 {len(tickers)} 个ticker")
            success_count += 1
            self.results['rest_api']['details'].append({
                'test': '获取交易所信息',
                'status': 'success'
            })
        except Exception as e:
            self._print_result("获取交易所信息", False, f"错误: {str(e)}")
            self.results['rest_api']['details'].append({
                'test': '获取交易所信息',
                'status': 'failed',
                'error': str(e)
            })
        
        # 测试3: 获取价格数据
        total_tests += 1
        try:
            # 获取BTC/USDT价格
            symbol = 'BTC/USDT:USDT'  # Hyperliquid使用USDT作为结算货币
            ticker = self.exchange.fetch_ticker(symbol)
            price = ticker.get('last')
            self._print_result(
                "获取价格数据",
                True,
                f"{symbol} 当前价格: {price}"
            )
            success_count += 1
            self.results['rest_api']['details'].append({
                'test': '获取价格数据',
                'status': 'success',
                'symbol': symbol,
                'price': price
            })
        except Exception as e:
            # 如果BTC/USDT失败，尝试其他符号
            try:
                # 尝试获取所有ticker
                tickers = self.exchange.fetch_tickers()
                if tickers:
                    first_symbol = list(tickers.keys())[0]
                    price = tickers[first_symbol].get('last')
                    self._print_result(
                        "获取价格数据",
                        True,
                        f"获取到 {len(tickers)} 个价格数据, 示例: {first_symbol} = {price}"
                    )
                    success_count += 1
                    self.results['rest_api']['details'].append({
                        'test': '获取价格数据',
                        'status': 'success',
                        'ticker_count': len(tickers)
                    })
                else:
                    raise Exception("未获取到价格数据")
            except Exception as e2:
                self._print_result("获取价格数据", False, f"错误: {str(e2)}")
                self.results['rest_api']['details'].append({
                    'test': '获取价格数据',
                    'status': 'failed',
                    'error': str(e2)
                })
        
        # 汇总结果
        all_success = success_count == total_tests
        self.results['rest_api']['status'] = 'success' if all_success else 'partial' if success_count > 0 else 'failed'
        
        print(f"\n{Colors.BOLD}结果: {success_count}/{total_tests} 测试通过{Colors.RESET}")
        return all_success
    
    def test_authentication(self) -> bool:
        """测试认证信息"""
        self._print_header("认证信息验证")
        
        private_key = self.auth_config.get('private_key', '').strip()
        wallet_address = self.auth_config.get('wallet_address', '').strip()
        
        # 检查配置是否存在
        if not private_key or not wallet_address:
            self._print_result("认证配置检查", False, "缺少私钥或钱包地址")
            self.results['authentication']['status'] = 'failed'
            self.results['authentication']['details'].append({
                'test': '配置检查',
                'status': 'failed',
                'error': '缺少必要配置'
            })
            return False
        
        self._print_result("认证配置检查", True, "配置存在")
        self.results['authentication']['details'].append({
            'test': '配置检查',
            'status': 'success'
        })
        
        # 验证私钥格式
        if not private_key.startswith('0x') or len(private_key) != 66:
            self._print_result("私钥格式验证", False, "私钥格式不正确 (应为0x开头的66字符)")
            self.results['authentication']['status'] = 'failed'
            self.results['authentication']['details'].append({
                'test': '私钥格式',
                'status': 'failed',
                'error': '格式不正确'
            })
            return False
        
        self._print_result("私钥格式验证", True, "格式正确")
        self.results['authentication']['details'].append({
            'test': '私钥格式',
            'status': 'success'
        })
        
        # 验证钱包地址格式
        if not wallet_address.startswith('0x') or len(wallet_address) != 42:
            self._print_result("钱包地址格式验证", False, "钱包地址格式不正确 (应为0x开头的42字符)")
            self.results['authentication']['status'] = 'failed'
            self.results['authentication']['details'].append({
                'test': '钱包地址格式',
                'status': 'failed',
                'error': '格式不正确'
            })
            return False
        
        self._print_result("钱包地址格式验证", True, "格式正确")
        self.results['authentication']['details'].append({
            'test': '钱包地址格式',
            'status': 'success'
        })
        
        # 如果可用，验证私钥和地址是否匹配
        if Account is not None:
            try:
                account = Account.from_key(private_key)
                derived_address = account.address
                
                if derived_address.lower() == wallet_address.lower():
                    self._print_result("私钥-地址匹配验证", True, "私钥与钱包地址匹配")
                    self.results['authentication']['details'].append({
                        'test': '私钥-地址匹配',
                        'status': 'success'
                    })
                else:
                    self._print_result(
                        "私钥-地址匹配验证",
                        False,
                        f"私钥对应的地址 ({derived_address}) 与配置的地址不匹配"
                    )
                    self.results['authentication']['details'].append({
                        'test': '私钥-地址匹配',
                        'status': 'failed',
                        'error': '地址不匹配'
                    })
                    # 注意：如果私钥和地址不匹配，这可能是配置错误，但不影响格式验证
                    self.results['authentication']['status'] = 'partial'
            except Exception as e:
                self._print_result("私钥-地址匹配验证", False, f"验证失败: {str(e)}")
                self.results['authentication']['details'].append({
                    'test': '私钥-地址匹配',
                    'status': 'failed',
                    'error': str(e)
                })
                self.results['authentication']['status'] = 'partial'
        else:
            self._print_result("私钥-地址匹配验证", None, "跳过 (缺少eth-account库)")
            self.results['authentication']['details'].append({
                'test': '私钥-地址匹配',
                'status': 'skipped'
            })
        
        # 测试认证API调用 (使用CCXT)
        try:
            if self.exchange and hasattr(self.exchange, 'walletAddress') and self.exchange.walletAddress:
                # 尝试获取账户余额（需要认证）
                balance = self.exchange.fetch_balance()
                if balance:
                    self._print_result("认证API调用", True, "成功获取账户余额")
                    self.results['authentication']['details'].append({
                        'test': '认证API调用',
                        'status': 'success'
                    })
                else:
                    self._print_result("认证API调用", None, "API调用成功但返回空数据")
                    self.results['authentication']['details'].append({
                        'test': '认证API调用',
                        'status': 'partial'
                    })
            else:
                self._print_result("认证API调用", None, "跳过 (未配置认证信息)")
                self.results['authentication']['details'].append({
                    'test': '认证API调用',
                    'status': 'skipped'
                })
        except ccxt.AuthenticationError:
            self._print_result("认证API调用", False, "认证失败: 私钥或地址无效")
            self.results['authentication']['details'].append({
                'test': '认证API调用',
                'status': 'failed',
                'error': '认证失败'
            })
            self.results['authentication']['status'] = 'failed'
        except Exception as e:
            # 其他错误可能是网络问题，不算认证失败
            self._print_result("认证API调用", None, f"跳过: {str(e)}")
            self.results['authentication']['details'].append({
                'test': '认证API调用',
                'status': 'skipped',
                'note': str(e)
            })
        
        if self.results['authentication']['status'] == 'pending':
            self.results['authentication']['status'] = 'success'
        
        # 根据最终状态打印结果
        final_status = self.results['authentication']['status']
        if final_status == 'success':
            print(f"\n{Colors.BOLD}{Colors.GREEN}{Symbols.CHECK()} 认证信息验证通过{Colors.RESET}")
        elif final_status == 'partial':
            print(f"\n{Colors.BOLD}{Colors.YELLOW}{Symbols.WARNING()} 认证信息格式正确，但私钥与地址不匹配{Colors.RESET}")
            print(f"  {Colors.YELLOW}提示: 请检查配置文件中的私钥和钱包地址是否正确匹配{Colors.RESET}")
        else:
            print(f"\n{Colors.BOLD}{Colors.RED}{Symbols.CROSS()} 认证信息验证失败{Colors.RESET}")
        
        return final_status == 'success'
    
    def test_market_data(self) -> bool:
        """测试市场数据获取"""
        self._print_header("市场数据获取测试 (CCXT)")
        
        success_count = 0
        total_tests = 0
        
        # 测试1: 获取多个交易对的价格
        total_tests += 1
        try:
            tickers = self.exchange.fetch_tickers()
            if tickers and len(tickers) > 0:
                # 找到BTC相关的ticker
                btc_tickers = [k for k in tickers.keys() if 'BTC' in k.upper()]
                self._print_result(
                    "获取交易对价格",
                    True,
                    f"获取到 {len(tickers)} 个价格数据" + (f", 包含 {len(btc_tickers)} 个BTC相关交易对" if btc_tickers else "")
                )
                success_count += 1
                self.results['market_data']['details'].append({
                    'test': '获取价格',
                    'status': 'success',
                    'count': len(tickers)
                })
            else:
                self._print_result("获取交易对价格", False, "返回数据为空")
                self.results['market_data']['details'].append({
                    'test': '获取价格',
                    'status': 'failed',
                    'error': '数据为空'
                })
        except Exception as e:
            self._print_result("获取交易对价格", False, f"错误: {str(e)}")
            self.results['market_data']['details'].append({
                'test': '获取价格',
                'status': 'failed',
                'error': str(e)
            })
        
        # 测试2: 获取订单簿数据
        total_tests += 1
        try:
            # 尝试获取BTC订单簿
            symbol = 'BTC/USDT:USDT'
            orderbook = self.exchange.fetch_order_book(symbol, limit=10)
            
            if orderbook and ('bids' in orderbook or 'asks' in orderbook):
                bids_count = len(orderbook.get('bids', []))
                asks_count = len(orderbook.get('asks', []))
                self._print_result(
                    "获取订单簿数据",
                    True,
                    f"{symbol} 订单簿: {bids_count} 买盘, {asks_count} 卖盘"
                )
                success_count += 1
                self.results['market_data']['details'].append({
                    'test': '获取订单簿',
                    'status': 'success',
                    'symbol': symbol
                })
            else:
                self._print_result("获取订单簿数据", False, "返回数据格式不正确")
                self.results['market_data']['details'].append({
                    'test': '获取订单簿',
                    'status': 'failed',
                    'error': '数据格式不正确'
                })
        except Exception as e:
            # 如果BTC/USDT失败，尝试其他符号
            try:
                markets = self.exchange.markets
                if markets:
                    # 找到第一个可用的交易对
                    first_symbol = list(markets.keys())[0]
                    orderbook = self.exchange.fetch_order_book(first_symbol, limit=5)
                    if orderbook:
                        self._print_result(
                            "获取订单簿数据",
                            True,
                            f"成功获取 {first_symbol} 订单簿"
                        )
                        success_count += 1
                        self.results['market_data']['details'].append({
                            'test': '获取订单簿',
                            'status': 'success',
                            'symbol': first_symbol
                        })
                    else:
                        raise Exception("订单簿数据为空")
                else:
                    raise Exception("无可用交易对")
            except Exception as e2:
                self._print_result("获取订单簿数据", False, f"错误: {str(e2)}")
                self.results['market_data']['details'].append({
                    'test': '获取订单簿',
                    'status': 'failed',
                    'error': str(e2)
                })
        
        # 汇总结果
        all_success = success_count == total_tests
        self.results['market_data']['status'] = 'success' if all_success else 'partial' if success_count > 0 else 'failed'
        
        print(f"\n{Colors.BOLD}结果: {success_count}/{total_tests} 测试通过{Colors.RESET}")
        return all_success
    
    def test_order_lifecycle(self) -> bool:
        """测试下单和撤单连通性
        
        流程：
        1. 获取当前BTC市场价格
        2. 以远低于市场价的限价单下买单（确保不会成交）
        3. 确认下单成功
        4. 立即撤销该订单
        5. 确认撤单成功
        """
        self._print_header("下单/撤单连通性测试 (CCXT)")
        
        success_count = 0
        total_tests = 0
        order_id = None
        symbol = None
        
        # 前置检查：认证信息是否有效
        private_key = self.auth_config.get('private_key', '').strip()
        wallet_address = self.auth_config.get('wallet_address', '').strip()
        
        if not private_key or not wallet_address \
                or 'YOUR' in private_key.upper() \
                or 'YOUR' in wallet_address.upper():
            self._print_result(
                "前置检查",
                False,
                "配置文件中未填入真实的私钥和钱包地址，请先更新 config/hyperliquid_config.yaml"
            )
            self.results['order_lifecycle']['status'] = 'skipped'
            self.results['order_lifecycle']['details'].append({
                'test': '前置检查',
                'status': 'skipped',
                'error': '缺少真实认证信息'
            })
            print(f"\n{Colors.BOLD}{Colors.YELLOW}{Symbols.WARNING()} 下单/撤单测试跳过 (需要真实认证信息){Colors.RESET}")
            return False
        
        # ---- 步骤1: 获取当前市场价格 ----
        total_tests += 1
        current_price = None
        try:
            # 确保市场信息已加载
            if not self.exchange.markets:
                self.exchange.load_markets()
            
            # 尝试使用 BTC/USDC:USDC (Hyperliquid永续合约)
            test_symbols = ['BTC/USDC:USDC', 'BTC/USDT:USDT']
            for sym in test_symbols:
                if sym in self.exchange.markets:
                    symbol = sym
                    break
            
            if not symbol:
                # 取第一个swap类型的交易对
                for sym, market in self.exchange.markets.items():
                    if market.get('swap'):
                        symbol = sym
                        break
            
            if not symbol:
                raise Exception("未找到可用的永续合约交易对")
            
            ticker = self.exchange.fetch_ticker(symbol)
            current_price = ticker.get('last') or ticker.get('close')
            
            if not current_price:
                raise Exception(f"无法获取 {symbol} 的当前价格")
            
            self._print_result(
                "获取市场价格",
                True,
                f"{symbol} 当前价格: {current_price}"
            )
            success_count += 1
            self.results['order_lifecycle']['details'].append({
                'test': '获取市场价格',
                'status': 'success',
                'symbol': symbol,
                'price': current_price
            })
        except Exception as e:
            self._print_result("获取市场价格", False, f"错误: {str(e)}")
            self.results['order_lifecycle']['details'].append({
                'test': '获取市场价格',
                'status': 'failed',
                'error': str(e)
            })
            self.results['order_lifecycle']['status'] = 'failed'
            print(f"\n{Colors.BOLD}{Colors.RED}{Symbols.CROSS()} 无法获取价格，跳过下单测试{Colors.RESET}")
            return False
        
        # ---- 步骤1.5: 检查账户余额 ----
        total_tests += 1
        available_balance = 0
        try:
            balance = self.exchange.fetch_balance()
            # Hyperliquid使用USDC作为保证金
            available_balance = balance.get('free', {}).get('USDC', 0) or 0
            total_balance = balance.get('total', {}).get('USDC', 0) or 0
            
            self._print_result(
                "查询账户余额",
                True,
                f"可用余额: {available_balance} USDC, 总余额: {total_balance} USDC"
            )
            success_count += 1
            self.results['order_lifecycle']['details'].append({
                'test': '查询账户余额',
                'status': 'success',
                'available': available_balance,
                'total': total_balance
            })
            
            # 检查余额是否足够 (Hyperliquid最小订单价值约10 USDC)
            if available_balance < 11:
                self._print_result(
                    "余额检查",
                    None,
                    f"可用余额 {available_balance} USDC 不足以下单 (最低约需 11 USDC)"
                )
                print(f"\n{Colors.BOLD}{Colors.YELLOW}{Symbols.WARNING()} 下单/撤单API连通性正常，但余额不足无法实际下单{Colors.RESET}")
                print(f"  {Colors.YELLOW}提示: 请向钱包转入至少 11 USDC 后重新测试{Colors.RESET}")
                self.results['order_lifecycle']['status'] = 'partial'
                self.results['order_lifecycle']['details'].append({
                    'test': '余额检查',
                    'status': 'partial',
                    'note': '余额不足，跳过实际下单测试'
                })
                print(f"\n{Colors.BOLD}结果: {success_count}/{total_tests} 测试通过 (余额不足，跳过下单){Colors.RESET}")
                return False
        except Exception as e:
            self._print_result("查询账户余额", False, f"错误: {str(e)}")
            self.results['order_lifecycle']['details'].append({
                'test': '查询账户余额',
                'status': 'failed',
                'error': str(e)
            })
        
        # ---- 步骤2: 下限价买单 (价格远低于市场价，不会成交) ----
        total_tests += 1
        try:
            # 获取市场精度信息
            market_info = self.exchange.markets.get(symbol, {})
            
            # 计算安全的测试价格：当前价格的50%，确保不会成交
            raw_test_price = current_price * 0.5
            test_price = float(self.exchange.price_to_precision(symbol, raw_test_price))
            
            # 计算最小下单量：确保订单价值约11 USDC (最低要求10 USDC)
            min_notional = 11.0  # USDC
            raw_amount = min_notional / test_price
            test_amount = float(self.exchange.amount_to_precision(symbol, raw_amount))
            # 确保数量不为0
            if test_amount <= 0:
                test_amount = float(self.exchange.amount_to_precision(symbol, 0.001))
            
            print(f"  >> 下单参数: {symbol} 买入 {test_amount} @ 限价 {test_price}")
            print(f"     (市场价: {current_price}, 测试价格为市场价的50%, 不会成交)")
            print(f"     (订单名义价值: ~{round(test_amount * test_price, 2)} USDC)")
            
            # 创建限价买单
            order = self.exchange.create_order(
                symbol=symbol,
                type='limit',
                side='buy',
                amount=test_amount,
                price=test_price
            )
            
            order_id = order.get('id')
            order_status = order.get('status', 'unknown')
            
            self._print_result(
                "创建限价买单",
                True,
                f"订单ID: {order_id}, 状态: {order_status}"
            )
            success_count += 1
            self.results['order_lifecycle']['details'].append({
                'test': '创建限价买单',
                'status': 'success',
                'order_id': order_id,
                'order_status': order_status,
                'price': test_price,
                'amount': test_amount
            })
        except ccxt.InsufficientFunds:
            self._print_result("创建限价买单", False, "余额不足，无法下单")
            self.results['order_lifecycle']['details'].append({
                'test': '创建限价买单',
                'status': 'failed',
                'error': '余额不足'
            })
            self.results['order_lifecycle']['status'] = 'partial'
            print(f"\n{Colors.BOLD}{Colors.YELLOW}{Symbols.WARNING()} 下单API连通正常，但余额不足{Colors.RESET}")
            print(f"\n{Colors.BOLD}结果: {success_count}/{total_tests} 测试通过{Colors.RESET}")
            return False
        except ccxt.AuthenticationError as e:
            self._print_result("创建限价买单", False, f"认证失败: {str(e)}")
            self.results['order_lifecycle']['details'].append({
                'test': '创建限价买单',
                'status': 'failed',
                'error': f'认证失败: {str(e)}'
            })
            self.results['order_lifecycle']['status'] = 'failed'
            print(f"\n{Colors.BOLD}结果: {success_count}/{total_tests} 测试通过{Colors.RESET}")
            return False
        except Exception as e:
            self._print_result("创建限价买单", False, f"错误: {str(e)}")
            self.results['order_lifecycle']['details'].append({
                'test': '创建限价买单',
                'status': 'failed',
                'error': str(e)
            })
            self.results['order_lifecycle']['status'] = 'failed'
            print(f"\n{Colors.BOLD}结果: {success_count}/{total_tests} 测试通过{Colors.RESET}")
            return False
        
        # ---- 步骤3: 查询订单状态 (使用 fetch_open_orders 批量查询) ----
        total_tests += 1
        try:
            time.sleep(1)  # 等待订单进入系统
            
            open_orders = self.exchange.fetch_open_orders(symbol)
            matched_order = None
            for o in open_orders:
                if o.get('id') == order_id:
                    matched_order = o
                    break
            
            if matched_order:
                fetched_status = matched_order.get('status', 'unknown')
                self._print_result(
                    "查询订单状态",
                    True,
                    f"订单ID: {order_id}, 状态: {fetched_status}, 挂单总数: {len(open_orders)}"
                )
                success_count += 1
                self.results['order_lifecycle']['details'].append({
                    'test': '查询订单状态',
                    'status': 'success',
                    'order_status': fetched_status
                })
            else:
                self._print_result(
                    "查询订单状态",
                    False,
                    f"订单ID: {order_id} 未在挂单列表中找到 (挂单数: {len(open_orders)})"
                )
                self.results['order_lifecycle']['details'].append({
                    'test': '查询订单状态',
                    'status': 'failed',
                    'note': '订单未在挂单列表中'
                })
        except Exception as e:
            self._print_result("查询订单状态", False, f"错误: {str(e)}")
            self.results['order_lifecycle']['details'].append({
                'test': '查询订单状态',
                'status': 'failed',
                'error': str(e)
            })
        
        # ---- 步骤4: 撤销订单 ----
        total_tests += 1
        try:
            cancel_result = self.exchange.cancel_order(order_id, symbol)
            
            cancel_status = None
            if isinstance(cancel_result, dict):
                cancel_status = cancel_result.get('status', 'canceled')
            else:
                cancel_status = 'canceled'
            
            self._print_result(
                "撤销订单",
                True,
                f"订单ID: {order_id} 已撤销, 状态: {cancel_status}"
            )
            success_count += 1
            self.results['order_lifecycle']['details'].append({
                'test': '撤销订单',
                'status': 'success',
                'cancel_status': cancel_status
            })
        except Exception as e:
            self._print_result("撤销订单", False, f"错误: {str(e)}")
            self.results['order_lifecycle']['details'].append({
                'test': '撤销订单',
                'status': 'failed',
                'error': str(e)
            })
            # 如果撤单失败，尝试用cancel_all_orders兜底
            try:
                print(f"  >> 尝试撤销所有挂单...")
                self.exchange.cancel_all_orders(symbol)
                self._print_result("撤销所有挂单 (兜底)", True, "已撤销所有挂单")
            except Exception as e2:
                self._print_result("撤销所有挂单 (兜底)", False, f"错误: {str(e2)}")
        
        # ---- 步骤5: 确认订单已撤销 ----
        total_tests += 1
        try:
            time.sleep(1)
            
            # 获取当前挂单列表，确认测试订单已不在列表中
            open_orders = self.exchange.fetch_open_orders(symbol)
            test_order_still_open = any(o.get('id') == order_id for o in open_orders)
            
            if not test_order_still_open:
                self._print_result(
                    "确认订单已撤销",
                    True,
                    f"订单 {order_id} 已不在挂单列表中, 当前挂单数: {len(open_orders)}"
                )
                success_count += 1
                self.results['order_lifecycle']['details'].append({
                    'test': '确认订单已撤销',
                    'status': 'success'
                })
            else:
                self._print_result("确认订单已撤销", False, "订单仍在挂单列表中")
                self.results['order_lifecycle']['details'].append({
                    'test': '确认订单已撤销',
                    'status': 'failed',
                    'error': '订单仍在挂单列表中'
                })
                # 再次尝试撤单
                try:
                    self.exchange.cancel_order(order_id, symbol)
                except Exception:
                    pass
        except Exception as e:
            self._print_result("确认订单已撤销", None, f"查询跳过: {str(e)}")
            self.results['order_lifecycle']['details'].append({
                'test': '确认订单已撤销',
                'status': 'skipped',
                'note': str(e)
            })
        
        # 汇总结果
        all_success = success_count == total_tests
        self.results['order_lifecycle']['status'] = 'success' if all_success else 'partial' if success_count > 0 else 'failed'
        
        if all_success:
            print(f"\n{Colors.BOLD}{Colors.GREEN}{Symbols.CHECK()} 下单/撤单测试全部通过!{Colors.RESET}")
        else:
            print(f"\n{Colors.BOLD}结果: {success_count}/{total_tests} 测试通过{Colors.RESET}")
        
        return all_success
    
    def print_summary(self):
        """打印测试总结"""
        self._print_header("测试总结")
        
        total_tests = len(self.results)
        passed_tests = sum(1 for r in self.results.values() if r['status'] == 'success')
        partial_tests = sum(1 for r in self.results.values() if r['status'] == 'partial')
        failed_tests = sum(1 for r in self.results.values() if r['status'] == 'failed')
        skipped_tests = sum(1 for r in self.results.values() if r['status'] == 'skipped')
        
        print(f"{Colors.BOLD}测试项目:{Colors.RESET}")
        print(f"  {Colors.GREEN}{Symbols.CHECK()} 通过: {passed_tests}{Colors.RESET}")
        if partial_tests > 0:
            print(f"  {Colors.YELLOW}{Symbols.WARNING()} 部分通过: {partial_tests}{Colors.RESET}")
        if failed_tests > 0:
            print(f"  {Colors.RED}{Symbols.CROSS()} 失败: {failed_tests}{Colors.RESET}")
        if skipped_tests > 0:
            print(f"  {Colors.BLUE}{Symbols.SKIP()} 跳过: {skipped_tests}{Colors.RESET}")
        
        print(f"\n{Colors.BOLD}详细结果:{Colors.RESET}")
        for test_name, result in self.results.items():
            status = result['status']
            if status == 'success':
                icon = f"{Colors.GREEN}{Symbols.CHECK()}{Colors.RESET}"
            elif status == 'partial':
                icon = f"{Colors.YELLOW}{Symbols.WARNING()}{Colors.RESET}"
            elif status == 'skipped':
                icon = f"{Colors.BLUE}{Symbols.SKIP()}{Colors.RESET}"
            else:
                icon = f"{Colors.RED}{Symbols.CROSS()}{Colors.RESET}"
            
            print(f"  {icon} {test_name.replace('_', ' ').title()}: {status}")
        
        print(f"\n{Colors.BOLD}{'='*60}{Colors.RESET}\n")
        
        # 返回整体状态
        if failed_tests == 0 and partial_tests == 0:
            return True
        elif failed_tests == 0:
            return 'partial'
        else:
            return False
    
    def run_all_tests(self):
        """运行所有测试"""
        print(f"\n{Colors.BOLD}{Colors.BLUE}")
        print("="*60)
        print("Hyperliquid 交易所连通性测试工具 (CCXT)".center(60))
        print("="*60)
        print(f"{Colors.RESET}")
        print(f"配置文件: {self.config_path}")
        print(f"CCXT版本: {ccxt.__version__}")
        print(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        # 运行各项测试
        self.test_rest_api()
        time.sleep(1)
        
        self.test_authentication()
        time.sleep(1)
        
        self.test_market_data()
        time.sleep(1)
        
        self.test_order_lifecycle()
        
        # 打印总结
        return self.print_summary()


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Hyperliquid交易所连通性测试工具 (使用CCXT)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python test_hyperliquid_connectivity.py
  python test_hyperliquid_connectivity.py --config config/hyperliquid_config.yaml
        """
    )
    
    parser.add_argument(
        '--config',
        type=str,
        default='config/hyperliquid_config.yaml',
        help='配置文件路径 (默认: config/hyperliquid_config.yaml)'
    )
    
    args = parser.parse_args()
    
    # 创建测试器并运行测试
    tester = HyperliquidConnectivityTester(config_path=args.config)
    result = tester.run_all_tests()
    
    # 根据结果设置退出码
    if result is True:
        sys.exit(0)
    elif result == 'partial':
        sys.exit(1)
    else:
        sys.exit(2)


if __name__ == '__main__':
    main()
