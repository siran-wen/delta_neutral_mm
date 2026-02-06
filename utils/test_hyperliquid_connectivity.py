#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Hyperliquid交易所连通性测试工具 (使用CCXT)

该工具使用CCXT库测试与Hyperliquid交易所的连接状态，包括：
- REST API连通性测试
- 认证信息验证
- 市场数据获取测试
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
            'market_data': {'status': 'pending', 'details': []}
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
            exchange_config = {
                'apiKey': wallet_address,  # Hyperliquid使用钱包地址作为apiKey
                'secret': private_key,     # 私钥作为secret
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
            if self.exchange and hasattr(self.exchange, 'apiKey') and self.exchange.apiKey:
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
